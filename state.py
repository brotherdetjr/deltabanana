import datetime
import functools
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from random import shuffle
from typing import Any

from telegram.error import BadRequest

from cfg import config
from gitsource import GitFileLink
from timeinterval import TimeInterval


@dataclass(frozen=True)
class Entry:
    studied: str
    native: str
    pronunciation: str | None = field(default=None)
    author: str | None = field(default=None)

    @functools.cache
    def __getitem__(self, idx: int) -> str | None:
        value = (self.studied, self.native, self.pronunciation)[idx] if 0 <= idx < 3 else None
        return value.strip() if value else None

    @functools.cache
    def __iter__(self) -> iter:
        return iter((self.studied, self.native, self.pronunciation, self.author))


@dataclass(frozen=True)
class Collection:
    entries: tuple[Entry]
    native_lang: str
    studied_lang: str
    topic: str
    link: GitFileLink

    @property
    def title(self) -> str:
        for idx, c in enumerate(config.collections):
            if c.as_git_file_link == self.link:
                return c.title
        raise IndexError()

    @property
    def decorated_title(self) -> str:
        return f'{self.title} {self.native_lang} {self.studied_lang}'


class UserState:
    __collection: Collection | None
    __mutable_entries: list[Entry]
    __entry_idx: int
    __tuple_idx: int
    __chat_id: int
    __reverse_mode: bool
    __last_interaction_time: datetime
    __nudge_time_interval: TimeInterval | None
    __nudge_menu_msg: Any

    def __init__(self, chat_id: int):
        self.__chat_id = chat_id
        self.__nudge_time_interval = None
        self.__nudge_menu_msg = None
        self.reset()

    @property
    def chat_id(self) -> int:
        return self.__chat_id

    def reset(self) -> None:
        self.__collection = None
        self.__reverse_mode = False
        self.__last_interaction_time = datetime.now()

    def reset_nudge(self) -> None:
        self.__nudge_time_interval = None

    def set_nudge(self) -> None:
        self.__nudge_time_interval = TimeInterval(
            datetime.now().time(),
            timedelta(seconds=config.nudge.active_interval_seconds)
        )

    @property
    def nudge_is_set(self) -> bool:
        return self.__nudge_time_interval is not None

    @property
    def collection(self) -> Collection | None:
        return self.__collection

    @collection.setter
    def collection(self, collection: Collection) -> None:
        self.__collection = collection
        self.__mutable_entries = list(collection.entries)
        self.shuffle_entries()

    @property
    def has_entries(self) -> bool:
        return len(self.__mutable_entries) > 0

    @property
    def current_word(self) -> str | None:
        return self.__mutable_entries[self.__entry_idx][self.__tuple_idx_with_mode_applied]

    @property
    def reverse_mode(self) -> bool:
        return self.__reverse_mode

    def toggle_reverse_mode(self) -> None:
        self.__reverse_mode = not self.__reverse_mode
        self.reset_collection()

    def go_next_word(self) -> None:
        self.__tuple_idx = self.__tuple_idx + 1
        if self.__tuple_idx == 3:
            self.__tuple_idx = 0
            self.__entry_idx = self.__entry_idx + 1
            if self.__entry_idx == len(self.__mutable_entries):
                self.__entry_idx = 0

    def roll(self, stick_to_question: bool) -> None:
        while not self.current_word or stick_to_question and self.__tuple_idx_with_mode_applied > 0:
            self.go_next_word()

    def reset_collection(self) -> None:
        self.__entry_idx = 0
        self.__tuple_idx = 0

    def shuffle_entries(self) -> None:
        shuffle(self.__mutable_entries)
        self.reset_collection()

    @property
    def decorated_word(self) -> str:
        c = self.collection
        native_studied_pronunciation_icon = [c.studied_lang, c.native_lang, '👂'][self.__tuple_idx_with_mode_applied]
        question_answer_icon = '❓' if self.__tuple_idx == 0 else ''
        return f'{native_studied_pronunciation_icon} {self.current_word} {question_answer_icon}'

    @property
    def idling(self) -> bool:
        return datetime.now() - self.__last_interaction_time > timedelta(seconds=config.nudge.idling_interval_seconds)

    @property
    def is_nudge_time(self) -> bool:
        if not self.__nudge_time_interval:
            return False
        return self.__nudge_time_interval.covers(datetime.now())

    def update_last_interaction_time(self) -> None:
        self.__last_interaction_time = datetime.now()

    @property
    def __tuple_idx_with_mode_applied(self) -> int:
        return 1 - self.__tuple_idx if self.__tuple_idx < 2 and self.__reverse_mode else self.__tuple_idx

    @property
    def nudge_menu_msg(self) -> Any:
        return self.__nudge_menu_msg

    @nudge_menu_msg.setter
    def nudge_menu_msg(self, msg: Any) -> None:
        self.__nudge_menu_msg = msg

    async def delete_nudge_menu(self) -> None:
        if self.__nudge_menu_msg is not None:
            try:
                await self.__nudge_menu_msg.delete()
                self.__nudge_menu_msg = None
            except BadRequest:
                pass
