import hashlib
import logging
import os.path
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Callable, TypeVar

from dulwich import porcelain
from dulwich.porcelain import NoneStream

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
class _CachedFiles:
    rev: str
    lock: RLock
    content: dict[str, Any] = field(default_factory=lambda: {})


T = TypeVar("T")


class GitSource:
    __link_cache: RefreshCache[_GitRepoLink, _CachedFiles]
    __refresh_callback: Callable[[str, str], None]

    def __init__(
            self,
            refresh_callback: Callable[[str, str], None],
            refresh_rate_seconds: int = 600
    ) -> None:
        self.__refresh_callback = refresh_callback
        self.__link_cache = RefreshCache(
            load_func=self.__sync_repo,
            refresh_callback=self.__on_refresh,
            refresh_rate_seconds=refresh_rate_seconds
        )

    def get(self, link: GitFileLink, map_func: Callable[[Path, GitFileLink], T]) -> T:
        repo_link = _GitRepoLink(link.url, link.branch)
        with self.__link_cache.get(repo_link).lock:
            # Lock doesn't change when created, but self.__link_cache.get(repo_link)
            # can change between a retrieval of the lock from cache and lock acquisition itself.
            # So 'doubled' self.__link_cache.get(repo_link) is needed.
            content: dict[str, T] = self.__link_cache.get(repo_link).content
            value = content.get(link.path)
            if value is not None:
                return value
            else:
                new_value = map_func(Path(link.dir_name(), link.path), link)
                content[link.path] = new_value
                return new_value

    @staticmethod
    def __sync_repo(link: _GitRepoLink, old_files: _CachedFiles) -> _CachedFiles:
        lock = old_files.lock if old_files else RLock()
        with lock:
            dir_name = link.dir_name()
            if os.path.isdir(dir_name):
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
            return _CachedFiles(rev, lock)

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
