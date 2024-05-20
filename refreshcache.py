import time
from threading import Lock, Thread
from typing import Generic, Callable, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class RefreshCache(Generic[K, V]):
    general_lock: Lock
    locks: dict[K, Lock]
    cache: dict[K, V]
    load_func: Callable[[K], V]
    refresh_callback: Callable[[K, V], None]
    refresh_rate_seconds: int

    def __init__(
            self,
            load_func: Callable[[K], V],
            refresh_callback: Callable[[K, V], None],
            refresh_rate_seconds: int
    ) -> None:
        self.general_lock = Lock()
        self.locks = {}
        self.cache = {}
        self.load_func = load_func
        self.refresh_callback = refresh_callback
        self.refresh_rate_seconds = refresh_rate_seconds

    def get(self, key: K) -> V:
        if key in self.cache:
            return self.cache[key]
        else:
            return self.__load_and_save__(key)

    def __load_and_save__(self, key: K) -> V:
        (lock, just_created) = self.__get_lock__(key)
        with lock:
            if just_created:
                # TODO graceful shutdown & destructor (?)
                Thread(daemon=True, target=lambda: self.__schedule__(key)).start()
            new_value: V = self.load_func(key)
            self.cache[key] = new_value
            return new_value

    def __refresh__(self, key: K) -> None:
        self.refresh_callback(key, self.__load_and_save__(key))

    def __get_lock__(self, key: K) -> (Lock, bool):
        just_created: bool = False
        with self.general_lock:
            if key not in self.locks:
                self.locks[key] = Lock()
                just_created = True
            return self.locks[key], just_created

    def __schedule__(self, key: K) -> None:
        while True:
            time.sleep(self.refresh_rate_seconds)
            self.__refresh__(key)
