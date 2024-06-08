import hashlib
import logging
import os.path
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from threading import RLock
from typing import Any, Callable, TypeVar, List

from dulwich import porcelain
from dulwich.porcelain import NoneStream, Error

from caches import RefreshCache

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _GitRepoLink:
    url: str
    branch: str

    def dir_name(self) -> str:
        return '.gitlink_' + hashlib.md5(f'{self.url}:{self.branch}'.encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class GitFileLink(_GitRepoLink):
    path: str


@dataclass(frozen=True)
class _Change:
    path: str
    content: Any


@dataclass(frozen=True)
class _CachedFiles:
    rev: str
    lock: RLock
    content: dict[str, Any]
    changes: List[_Change]


T = TypeVar("T")


class GitSource:
    __link_cache: RefreshCache[_GitRepoLink, _CachedFiles]
    __refresh_callback: Callable[[str, str], None]
    __apply_changes_callback: Callable[[List[Any], str], None]
    __no_change_sync_interval_multiplier: int
    __commit_message: str | None
    __sync_skip_count: int

    def __init__(
            self,
            refresh_callback: Callable[[str, str], None],
            apply_changes_callback: Callable[[List[Any], str], None],
            sync_interval_seconds: int = 60,
            no_change_sync_interval_multiplier: int = 10,
            commit_message: str | None = None,
    ) -> None:
        self.__refresh_callback = refresh_callback
        self.__apply_changes_callback = apply_changes_callback
        self.__no_change_sync_interval_multiplier = no_change_sync_interval_multiplier
        self.__commit_message = commit_message
        self.__sync_skip_count = 0
        self.__link_cache = RefreshCache(
            load_func=self.__sync_repo,
            refresh_callback=self.__on_refresh,
            sync_interval_seconds=sync_interval_seconds
        )

    def get(self, link: GitFileLink, map_func: Callable[[Path, GitFileLink], T]) -> T:
        return self.__locked(link, lambda f: GitSource.__get(link, f.content, map_func))

    def register_change(self, link: GitFileLink, change_content: Any) -> None:
        self.__locked(link, lambda cached_files: GitSource.__register_change(link.path, change_content, cached_files))

    def __locked(self, link: GitFileLink, action: Callable[[_CachedFiles], T]) -> T:
        repo_link = _GitRepoLink(link.url, link.branch)
        with self.__link_cache.get(repo_link).lock:
            # Lock doesn't change when created, but self.__link_cache.get(repo_link)
            # can change between a retrieval of the lock from cache and lock acquisition itself.
            # So 'doubled' self.__link_cache.get(repo_link) is needed.
            return action(self.__link_cache.get(repo_link))

    @staticmethod
    def __register_change(path: str, change_content: Any, cached_files: _CachedFiles) -> None:
        if path not in cached_files.content:
            raise LookupError()
        cached_files.changes.append(_Change(path, change_content))

    @staticmethod
    def __get(link: GitFileLink, content: dict[str, T], map_func: Callable[[Path, GitFileLink], T]) -> T:
        value = content.get(link.path)
        if value is not None:
            return value
        else:
            new_value = map_func(Path(link.dir_name(), link.path), link)
            content[link.path] = new_value
            return new_value

    def __sync_repo(self, link: _GitRepoLink, old_files: _CachedFiles) -> _CachedFiles:
        # noinspection PyBroadException
        try:
            lock = old_files.lock if old_files else RLock()
            with lock:
                if old_files and not old_files.changes and \
                        self.__sync_skip_count < self.__no_change_sync_interval_multiplier - 1:
                    self.__sync_skip_count += 1
                    return old_files
                changes = old_files.changes if old_files else []
                self.__sync_skip_count = 0
                dir_name = link.dir_name()
                if os.path.isdir(dir_name):
                    porcelain.reset(dir_name, 'hard')
                    logger.info(f'Pulling {link} at path {dir_name} ...')
                    porcelain.pull(dir_name, errstream=NoneStream())
                else:
                    logger.info(f'Cloning {link} at path {dir_name} ...')
                    porcelain.clone(
                        source=link.url,
                        target=dir_name,
                        branch=link.branch,
                        depth=1,
                        errstream=NoneStream()
                    )
                rev = GitSource.__get_commit(dir_name)
                logger.info(f'Updated  {link} at revision {rev}')
                for key, group in groupby(changes, lambda c: c.path):
                    self.__apply_changes_callback(list(map(lambda g: g.content, group)), key)
                try:
                    if porcelain.add(dir_name, paths=[dir_name])[1]:
                        porcelain.commit(dir_name, self.__commit_message)
                        porcelain.push(dir_name, errstream=NoneStream())
                        rev = GitSource.__get_commit(dir_name)
                        changes = []
                        logger.info(f'After push {link} is at revision {rev}')
                    elif changes:
                        logger.warning(f'Registered changes have not made any actual change for {link}')
                except Error:
                    logger.warning(f'Could not push changes for {link}, will retry later', exc_info=True)
                return _CachedFiles(rev, lock, {}, changes)
        except BaseException:
            logger.error(f'Failed syncing repo {link}', exc_info=True)

    def __on_refresh(self, link: _GitRepoLink, old_files: _CachedFiles | None, new_files: _CachedFiles) -> bool:
        if old_files and (old_files.rev == new_files.rev):
            return False
        self.__refresh_callback(link.url, link.branch)
        return True

    @staticmethod
    def __get_commit(repo_path: str) -> str:
        git_folder = Path(repo_path, '.git')
        head_name = Path(git_folder, 'HEAD').read_text().split('\n')[0].split(' ')[-1]
        head_ref = Path(git_folder, head_name)
        return head_ref.read_text().replace('\n', '')
