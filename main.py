import csv
import dataclasses
import gettext
import hashlib
import json
import logging
import os.path
import time
from dataclasses import dataclass, field
from random import shuffle
from threading import Lock, Thread
from typing import Final, List, Tuple, TypeVar, Generic, Callable, Any

import pathlib
import yaml
from cachetools import TTLCache
from dulwich import porcelain
from dulwich.porcelain import NoneStream
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, Application, CommandHandler, JobQueue

TOKEN: Final[str] = os.getenv('DELTABANANA_TOKEN')
BOT_USER: Final[str] = os.getenv('DELTABANANA_USER')
ICONS: Final[List[str]] = ['🇬🇧', '🇷🇺', '👂']

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


@dataclass(frozen=True)
class GitRef:
    url: str
    path: str
    branch: str = field(default='main')

    def md5(self) -> str:
        return hashlib.md5(json.dumps(dataclasses.asdict(self)).encode('utf-8')).hexdigest()


class UserState:
    current_ref: Any
    entries: List[Tuple[str]]
    entryIdx: int
    tupleIdx: int
    chat_id: int

    def __init__(self, entries: List[Tuple[str]], current_ref: Any, chat_id: int):
        self.set_entries(entries)
        self.current_ref = current_ref
        self.chat_id = chat_id

    def set_entries(self, entries: List[Tuple[str]]) -> None:
        self.entries = entries
        self.reset()

    def has_entries(self) -> bool:
        return len(self.entries) > 0

    def get_current_word(self) -> str:
        return self.entries[self.entryIdx][self.tupleIdx]

    def go_next_word(self) -> None:
        self.tupleIdx = self.tupleIdx + 1
        if self.tupleIdx == 3:
            self.tupleIdx = 0
            self.entryIdx = self.entryIdx + 1
            if self.entryIdx == len(self.entries):
                self.entryIdx = 0

    def reset(self) -> None:
        self.entryIdx = 0
        self.tupleIdx = 0

    def shuffle_entries(self) -> None:
        shuffle(self.entries)
        self.reset()


class CapacityException(Exception):
    pass


class LimitedTtlCache(TTLCache):
    def popitem(self):
        raise CapacityException()


class Main:
    user_states: LimitedTtlCache[str, UserState]
    collections: RefreshCache[GitRef, List[Tuple[str]]]
    app: Application

    def __init__(self):
        self.user_states = LimitedTtlCache(
            maxsize=config.get('active_user_sessions', {}).get('max_count', 1000),
            ttl=config.get('active_user_sessions', {}).get('inactivity_timeout_seconds', 604800)
        )
        app = Application.builder().token(TOKEN).job_queue(JobQueue()).build()
        app.add_handler(CommandHandler('start', self.start_command))
        app.add_handler(CommandHandler('next', self.next_command))
        app.add_handler(CommandHandler('shuffle', self.shuffle_command))
        app.add_error_handler(self.error)
        self.collections = RefreshCache(
            load_func=self.fetch_entries,
            refresh_callback=self.on_refresh,
            refresh_rate_seconds=config.get('collection_refresh_rate_seconds', 300)
        )
        self.app = app
        logger.info('Polling...')
        app.run_polling(poll_interval=config.get('bot_poll_interval_seconds', 2))

    def user_state(self, update: Update) -> UserState:
        username = update.effective_user.username
        if username not in self.user_states:
            ref = GitRef('git@github.com:brotherdetjr/deltabanana-collections.git', 'helloworld')
            self.user_states[username] = UserState(self.collections.get(ref), ref, update.effective_chat.id)
            logger.info(f'Storing a new state for user {username}. Stored state count: {len(self.user_states)}')
        else:
            # Refresh state's TTL
            self.user_states[username] = self.user_states[username]
        return self.user_states[username]

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.user_state(update).reset()
        button = [[KeyboardButton('/next')]]
        await update.effective_message.reply_text(
            _("Let's rock!"),
            reply_markup=ReplyKeyboardMarkup(button, resize_keyboard=True)
        )

    async def next_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.user_state(update).has_entries():
            await update.message.reply_text(_('No entries in collection!'))
            return

        text: str = self.user_state(update).get_current_word().strip()
        if not text:
            text = '...'
        await update.message.reply_text(f'{ICONS[self.user_state(update).tupleIdx]} {text}')
        self.user_state(update).go_next_word()

    async def shuffle_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.user_state(update).shuffle_entries()
        await update.message.reply_text(_('Shuffled'))

    async def error(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if isinstance(context.error, CapacityException):
            await update.message.reply_text(_('The bot is busy'))
        else:
            logger.error(f'Update {update} caused an error', context.error)

    @staticmethod
    def fetch_entries(ref: GitRef) -> List[Tuple[str]]:
        path = '.gitref_' + ref.md5()
        if os.path.isdir(path):
            logger.info(f'Updating {ref} at path {path} ...')
            porcelain.pull(path, errstream=NoneStream())
        else:
            logger.info(f'Cloning {ref} at path {path} ...')
            porcelain.clone(
                source=ref.url,
                target=path,
                branch=ref.branch,
                depth=1,
                errstream=NoneStream()
            )
        entries = []
        with open(pathlib.PurePath(path, ref.path, 'entries.csv'), encoding='utf-8') as csvfile:
            entry_reader = csv.reader(csvfile, delimiter=';')
            for row in entry_reader:
                entries.append(tuple(row))
        logger.info(f'Fetched collection {ref}')
        return entries

    def on_refresh(self, ref: GitRef, entries: List[Tuple[str]]) -> None:
        for username in self.user_states:
            state: UserState = self.user_states.get(username)
            if ref == state.current_ref and entries != state.entries:
                state.set_entries(entries)
                self.app.job_queue.run_once(
                    lambda ignore: self.app.bot.send_message(
                        state.chat_id,
                        _('Word collection has been modified externally!')
                    ),
                    0
                )


if __name__ == '__main__':
    config: dict = yaml.safe_load(open('deltabanana.yaml'))
    if not config:
        config = {}
    _ = gettext.translation('deltabanana', './locales', fallback=False, languages=[config.get('locale', 'en')]).gettext
    logger = logging.getLogger('deltabanana')
    logger.setLevel(level=logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.info('Starting bot...')
    Main()
