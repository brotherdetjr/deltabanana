import time
from threading import Lock, Thread
from typing import Generic, Callable, TypeVar

from cachetools import TTLCache

_K = TypeVar("_K")
_V = TypeVar("_V")


class RefreshCache(Generic[_K, _V]):
    __general_lock: Lock
    __locks: dict[_K, Lock]
    __cache: dict[_K, _V]
    __load_func: Callable[[_K], _V]
    __refresh_callback: Callable[[_K, _V], None]
    __refresh_rate_seconds: int

    def __init__(
            self,
            load_func: Callable[[_K], _V],
            refresh_callback: Callable[[_K, _V, _V], None],
            refresh_rate_seconds: int
    ) -> None:
        self.__general_lock = Lock()
        self.__locks = {}
        self.__cache = {}
        self.__load_func = load_func
        self.__refresh_callback = refresh_callback
        self.__refresh_rate_seconds = refresh_rate_seconds

    def get(self, key: _K) -> _V:
        if key in self.__cache:
            return self.__cache[key]
        else:
            return self.__load_and_save(key)

    def __load_and_save(self, key: _K) -> _V:
        (lock, just_created) = self.__get_lock(key)
        with lock:
            if just_created:
                # TODO graceful shutdown & destructor (?)
                Thread(daemon=True, target=lambda: self.__schedule(key)).start()
            new_value: _V = self.__load_func(key)
            self.__cache[key] = new_value
            return new_value

    def __refresh(self, key: _K) -> None:
        self.__refresh_callback(key, self.__cache[key], self.__load_and_save(key))

    def __get_lock(self, key: _K) -> (Lock, bool):
        just_created: bool = False
        with self.__general_lock:
            if key not in self.__locks:
                self.__locks[key] = Lock()
                just_created = True
            return self.__locks[key], just_created

    def __schedule(self, key: _K) -> None:
        while True:
            time.sleep(self.__refresh_rate_seconds)
            self.__refresh(key)


class LimitedTtlCache(TTLCache):
    def popitem(self):
        raise CapacityException()


class CapacityException(Exception):
    pass
