import time
from threading import RLock, Thread
from typing import Generic, Callable, TypeVar

from cachetools import TTLCache

_K = TypeVar("_K")
_V = TypeVar("_V")


class RefreshCache(Generic[_K, _V]):
    __general_lock: RLock
    __locks: dict[_K, RLock]
    __cache: dict[_K, _V]
    __load_func: Callable[[_K, _V | None], _V]
    __refresh_callback: Callable[[_K, _V | None, _V], bool]
    __refresh_rate_seconds: int

    def __init__(
            self,
            load_func: Callable[[_K, _V | None], _V],
            refresh_callback: Callable[[_K, _V | None, _V], bool],
            refresh_rate_seconds: int
    ) -> None:
        self.__general_lock = RLock()
        self.__locks = {}
        self.__cache = {}
        self.__load_func = load_func
        self.__refresh_callback = refresh_callback
        self.__refresh_rate_seconds = refresh_rate_seconds

    def get(self, key: _K) -> _V:
        (lock, just_created) = self.__get_lock(key)
        with lock:
            if just_created:
                # TODO graceful shutdown & destructor (?)
                Thread(daemon=True, target=lambda: self.__schedule(key)).start()
            if key in self.__cache:
                return self.__cache[key]
            else:
                return self.__load_and_save(key)

    def __load_and_save(self, key: _K) -> _V:
        old_value: _V = self.__cache.get(key)
        new_value: _V = self.__load_func(key, old_value)
        if self.__refresh_callback(key, old_value, new_value):
            self.__cache[key] = new_value
            return new_value
        else:
            return old_value

    def __get_lock(self, key: _K) -> (RLock, bool):
        just_created: bool = False
        with self.__general_lock:
            if key not in self.__locks:
                self.__locks[key] = RLock()
                just_created = True
            return self.__locks[key], just_created

    def __schedule(self, key: _K) -> None:
        while True:
            time.sleep(self.__refresh_rate_seconds)
            with self.__get_lock(key)[0]:
                self.__load_and_save(key)


class LimitedTtlCache(TTLCache):
    def popitem(self):
        raise CapacityException()


class CapacityException(Exception):
    pass
