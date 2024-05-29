import csv
import gettext
import logging
import os.path
from random import shuffle
from typing import Final, List, Tuple

import pathlib
import yaml
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, Application, CommandHandler, JobQueue

from caches import LimitedTtlCache, CapacityException
from gitsource import GitSource, GitFileLink

ICONS: Final[List[str]] = ['🇬🇧', '🇷🇺', '👂']


class UserState:
    current_link: GitFileLink
    entries: List[Tuple[str]]
    entryIdx: int
    tupleIdx: int
    chat_id: int

    def __init__(self, entries: List[Tuple[str]], current_link: GitFileLink, chat_id: int):
        self.set_entries(entries)
        self.current_link = current_link
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
    __git_source: GitSource
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
        self.__git_source = GitSource(
            refresh_callback=self.on_refresh,
            refresh_rate_seconds=config.get('entries_refresh_rate_seconds', 600)
        )
        self.app = app
        logger.info('Polling...')
        app.run_polling(poll_interval=config.get('bot_poll_interval_seconds', 2))

    def user_state(self, update: Update) -> UserState:
        username = update.effective_user.username
        if username not in self.user_states:
            link = GitFileLink('git@github.com:brotherdetjr/deltabanana-collections.git', 'helloworld/entries.csv')
            entries: List[Tuple[str]] = list(self.__git_source.get(link, Main.parse_entries))
            self.user_states[username] = UserState(entries, link, update.effective_chat.id)
            logger.info(f'Storing a new state for user {username}. Stored state count: {len(self.user_states)}')
        else:
            # Refresh state's TTL
            self.user_states[username] = self.user_states[username]
        return self.user_states[username]

    # noinspection PyUnusedLocal
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.user_state(update).reset()
        button = [[KeyboardButton('/next')]]
        await update.effective_message.reply_text(
            _("Let's rock!"),
            reply_markup=ReplyKeyboardMarkup(button, resize_keyboard=True)
        )

    # noinspection PyUnusedLocal
    async def next_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        if not state.has_entries():
            await update.message.reply_text(_('No entries in collection!'))
            return

        text: str = state.get_current_word().strip()
        while not text:
            state.go_next_word()
            text = state.get_current_word().strip()
        await update.message.reply_text(f'{ICONS[state.tupleIdx]} {text}')
        state.go_next_word()

    # noinspection PyUnusedLocal
    async def shuffle_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.user_state(update).shuffle_entries()
        await update.message.reply_text(_('Shuffled'))

    @staticmethod
    async def error(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if isinstance(context.error, CapacityException):
            await update.message.reply_text(_('The bot is busy'))
        else:
            logger.error(f'Update {update} caused an error', context.error)

    @staticmethod
    def parse_entries(path: pathlib.Path) -> Tuple[Tuple[str]]:
        entries = []
        with open(path, encoding='utf-8') as csvfile:
            for row in csv.reader(csvfile, delimiter=';'):
                entries.append(tuple(row))
        return tuple(entries)

    # noinspection PyUnusedLocal
    def on_refresh(self, url: str, branch: str) -> None:
        for username in self.user_states:
            state: UserState = self.user_states.get(username)
            link: GitFileLink = state.current_link
            if (url, branch) == (link.url, link.branch):
                state.set_entries(list(self.__git_source.get(link, Main.parse_entries)))
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
    Main(os.getenv('DELTABANANA_TOKEN'))
