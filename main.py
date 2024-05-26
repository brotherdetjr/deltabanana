import csv
import dataclasses
import gettext
import hashlib
import json
import logging
import os.path
from dataclasses import dataclass, field
from random import shuffle
from typing import Final, List, Tuple, Any

import pathlib
import yaml
from dulwich import porcelain
from dulwich.porcelain import NoneStream
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, Application, CommandHandler, JobQueue

from caches import RefreshCache, LimitedTtlCache, CapacityException

ICONS: Final[List[str]] = ['🇬🇧', '🇷🇺', '👂']


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


class Main:
    user_states: LimitedTtlCache[str, UserState]
    collections: RefreshCache[GitRef, List[Tuple[str]]]
    app: Application

    def __init__(self, bot_token: str):
        self.user_states = LimitedTtlCache(
            maxsize=config.get('active_user_sessions', {}).get('max_count', 1000),
            ttl=config.get('active_user_sessions', {}).get('inactivity_timeout_seconds', 604800)
        )
        app = Application.builder().token(bot_token).job_queue(JobQueue()).build()
        app.add_handler(CommandHandler('start', self.start_command))
        app.add_handler(CommandHandler('next', self.next_command))
        app.add_handler(CommandHandler('shuffle', self.shuffle_command))
        app.add_error_handler(self.error)
        self.collections = RefreshCache(
            load_func=self.fetch_entries,
            refresh_callback=self.on_refresh,
            refresh_rate_seconds=config.get('collection_refresh_rate_seconds', 600)
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
            for row in csv.reader(csvfile, delimiter=';'):
                entries.append(tuple(row))
        logger.info(f'Fetched collection {ref}')
        return entries

    def on_refresh(self, ref: GitRef, old_entries: List[Tuple[str]], new_entries: List[Tuple[str]]) -> bool:
        if new_entries == old_entries:
            return False
        for username in self.user_states:
            state: UserState = self.user_states.get(username)
            if ref == state.current_ref:
                state.set_entries(new_entries)
                self.app.job_queue.run_once(
                    lambda ignore: self.app.bot.send_message(
                        state.chat_id,
                        _('Word collection has been modified externally!')
                    ),
                    0
                )
        return True


if __name__ == '__main__':
    config: dict = yaml.safe_load(open('deltabanana.yaml'))
    if not config:
        config = {}
    _ = gettext.translation('deltabanana', './locales', fallback=False, languages=[config.get('locale', 'en')]).gettext
    logger = logging.getLogger('deltabanana')
    logger.setLevel(level=logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.info('Starting bot...')
    Main(os.getenv('DELTABANANA_TOKEN'))
