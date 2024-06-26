import hashlib
import logging
import os.path
from dataclasses import dataclass, field
from itertools import groupby
from pathlib import Path
from threading import RLock
from typing import Any, TypeVar
from collections.abc import Callable

import yaml
from dacite import from_dict
from dulwich import porcelain
from dulwich.porcelain import NoneStream, Error

from caches import RefreshCache

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class _GitRepoLink:
    url: str
    branch: str = field(default='main')

    def dir_name(self) -> str:
        return '.gitlink_' + hashlib.md5(f'{self.url}:{self.branch}'.encode('utf-8')).hexdigest()


# noinspection PyDataclass
@dataclass(frozen=True, kw_only=True)
class GitFileLink(_GitRepoLink):
    path: str


@dataclass(frozen=True)
class _Change:
    link: GitFileLink
    content: Any


@dataclass(frozen=True)
class _CachedFiles:
    rev: str
    lock: RLock
    content: dict[str, Any]
    changes: list[_Change]


T = TypeVar("T")


class GitSource:
    __link_cache: RefreshCache[_GitRepoLink, _CachedFiles]
    __refresh_callback: Callable[[str, str], None]
    __apply_changes_callback: Callable[[list[Any], GitFileLink], None]
    __no_change_sync_interval_multiplier: int
    __commit_message: str | None
    __sync_skip_count: int

    def __init__(
            self,
            refresh_callback: Callable[[str, str], None],
            apply_changes_callback: Callable[[list[Any], GitFileLink], None],
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

    def get(self, link: GitFileLink, mapping: Callable[[Path, GitFileLink], T] | type[T]) -> T:
        if type(mapping) is type:
            _T: type[T] = mapping

            # noinspection PyUnusedLocal
            def __parse_as_data_class(path: Path, ignore: GitFileLink) -> _T:
                with open(path, encoding='UTF-8') as file:
                    return from_dict(_T, yaml.safe_load(file))
            mapping = __parse_as_data_class
        return self.__locked(link, lambda f: GitSource.__get(link, f.content, mapping))

    def register_change(self, link: GitFileLink, change_content: Any) -> None:
        self.__locked(link, lambda cached_files: GitSource.__register_change(link, change_content, cached_files))

    def __locked(self, link: GitFileLink, action: Callable[[_CachedFiles], T]) -> T:
        repo_link = _GitRepoLink(url=link.url, branch=link.branch)
        with self.__link_cache.get(repo_link).lock:
            # Lock doesn't change when created, but self.__link_cache.get(repo_link)
            # can change between a retrieval of the lock from cache and lock acquisition itself.
            # So 'doubled' self.__link_cache.get(repo_link) is needed.
            return action(self.__link_cache.get(repo_link))

    @staticmethod
    def __register_change(link: GitFileLink, change_content: Any, cached_files: _CachedFiles) -> None:
        if link.path not in cached_files.content:
            raise LookupError()
        cached_files.changes.append(_Change(link, change_content))

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
                changes: list[_Change] = old_files.changes if old_files else []
                self.__sync_skip_count = 0
                dir_name = link.dir_name()
                if os.path.isdir(dir_name):
                    porcelain.clean(dir_name, dir_name)
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
                logger.info(f'Updated  {link} at revision {GitSource.__get_rev(dir_name)}')
                if changes:
                    self.__add_commit_push(link, changes)
                return _CachedFiles(GitSource.__get_rev(dir_name), lock, {}, changes)
        except BaseException:
            logger.error(f'Failed syncing repo {link}', exc_info=True)

    def __add_commit_push(self, link: _GitRepoLink, changes: list[_Change]) -> None:
        dir_name = link.dir_name()
        for key, group in groupby(changes, lambda c: c.link):
            self.__apply_changes_callback(list(map(lambda g: g.content, group)), key)
        try:
            repo = porcelain.open_repo(os.getcwd() + '/' + dir_name)
            status = porcelain.status(repo)
            to_stage = status.unstaged + status.untracked
            if to_stage:
                repo.stage(to_stage)
                porcelain.commit(repo, message=self.__commit_message)
                porcelain.push(repo, errstream=NoneStream())
                rev = GitSource.__get_rev('.')
                logger.info(f'After push {link} is at revision {rev}')
            elif changes:
                logger.warning(f'Registered changes have not made any actual change for {link}')
            changes.clear()
        except Error:
            logger.warning(f'Could not push changes for {link}, will retry later', exc_info=True)

    def __on_refresh(self, link: _GitRepoLink, old_files: _CachedFiles | None, new_files: _CachedFiles) -> bool:
        if old_files and (old_files.rev == new_files.rev):
            return False
        self.__refresh_callback(link.url, link.branch)
        return True

    @staticmethod
    def __get_rev(repo_path: str) -> str:
        git_folder = Path(repo_path, '.git')
        head_name = Path(git_folder, 'HEAD').read_text().split('\n')[0].split(' ')[-1]
        head_ref = Path(git_folder, head_name)
        return head_ref.read_text().replace('\n', '')
