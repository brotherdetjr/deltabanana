import csv
import gettext
import logging.config
import os.path
from dataclasses import dataclass
from random import shuffle
from typing import List, Tuple

import asyncio
import pathlib
import yaml
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, \
    ReplyKeyboardRemove
from telegram.ext import ContextTypes, Application, CommandHandler, JobQueue, CallbackQueryHandler

from caches import LimitedTtlCache, CapacityException
from gitsource import GitSource, GitFileLink

logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Collection:
    entries: Tuple[Tuple[str]]
    nativeLang: str
    studiedLang: str
    topic: str
    link: GitFileLink


class UserState:
    __collection: Collection | None
    __mutable_entries: List[Tuple[str]]
    __entryIdx: int
    __tupleIdx: int
    __chat_id: int

    def __init__(self, chat_id: int):
        self.__chat_id = chat_id

    @property
    def chat_id(self) -> int:
        return self.__chat_id

    def reset(self) -> None:
        self.__collection = None

    @property
    def collection(self) -> Collection | None:
        return self.__collection

    @collection.setter
    def collection(self, collection: Collection | None) -> None:
        self.__collection = collection
        if collection:
            self.__mutable_entries = list(collection.entries)
            self.shuffle_entries()

    @property
    def has_entries(self) -> bool:
        return len(self.__mutable_entries) > 0

    @property
    def current_word(self) -> str:
        return self.__mutable_entries[self.__entryIdx][self.__tupleIdx]

    def go_next_word(self) -> None:
        self.__tupleIdx = self.__tupleIdx + 1
        if self.__tupleIdx == 3:
            self.__tupleIdx = 0
            self.__entryIdx = self.__entryIdx + 1
            if self.__entryIdx == len(self.__mutable_entries):
                self.__entryIdx = 0

    def reset_collection(self) -> None:
        self.__entryIdx = 0
        self.__tupleIdx = 0

    def shuffle_entries(self) -> None:
        shuffle(self.__mutable_entries)
        self.reset_collection()

    @property
    def word_decoration(self) -> str:
        return [self.collection.studiedLang, self.collection.nativeLang, '👂'][self.__tupleIdx]


class Main:
    user_states: LimitedTtlCache[str, UserState]
    git_source: GitSource
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
        app.add_handler(CallbackQueryHandler(self.collection_button))
        app.add_error_handler(self.error)
        self.git_source = GitSource(
            refresh_callback=self.on_refresh,
            refresh_rate_seconds=config.get('collection_refresh_rate_seconds', 600)
        )
        self.app = app
        logger.info('Polling...')
        app.run_polling(poll_interval=config.get('bot_poll_interval_seconds', 2))

    def user_state(self, update: Update) -> UserState:
        username = update.effective_user.username
        if username not in self.user_states:
            self.user_states[username] = UserState(update.effective_chat.id)
            logger.info(f'Storing a new state for user {username}. Stored state count: {len(self.user_states)}')
        else:
            # Refresh state's TTL
            self.user_states[username] = self.user_states[username]
        return self.user_states[username]

    # noinspection PyUnusedLocal
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.user_state(update).reset()
        collections_keyboard = []
        for idx, ignore in enumerate(config['collections']):
            try:
                collection = self.get_collection(idx)
                button_markup = [InlineKeyboardButton(Main.to_title(collection, idx), callback_data=idx)]
                collections_keyboard.append(button_markup)
            except FileNotFoundError as e:
                logger.error('Failed to load collection #%s from config file', idx, exc_info=e)
        reply_markup = InlineKeyboardMarkup(collections_keyboard)
        await asyncio.gather(
            self.remove_chat_buttons(update.effective_chat.id),
            update.message.reply_text(_('Collections'), reply_markup=reply_markup)
        )

    @staticmethod
    def to_title(collection: Collection, index_in_config: int) -> str:
        return f"{config['collections'][index_in_config]['title']} {collection.nativeLang} {collection.studiedLang}"

    async def collection_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        idx: int = int(update.callback_query.data)
        collection = self.get_collection(idx)
        self.user_state(update).collection = collection
        await asyncio.gather(
            update.callback_query.answer(),
            update.effective_message.reply_text(
                _('Selected collection {topic}').format(topic=Main.to_title(collection, idx)),
                reply_markup=ReplyKeyboardMarkup(
                    [[KeyboardButton('/next')]],
                    resize_keyboard=True
                )
            )
        )
        await self.next_command(update, context)

    # noinspection PyUnusedLocal
    async def next_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        if not state.collection:
            await update.message.reply_text(_('Choose collection first!'))
            return
        if not state.has_entries:
            await update.message.reply_text(_('No entries in collection!'))
            return

        text: str = state.current_word.strip()
        while not text:
            state.go_next_word()
            text = state.current_word.strip()
        await self.app.bot.send_message(state.chat_id, f'{state.word_decoration} {text}')
        state.go_next_word()

    # noinspection PyUnusedLocal
    def get_collection(self, idx) -> Collection:
        c = config['collections'][idx]
        link = GitFileLink(c['url'], c['path'], c.get('branch'))
        return self.git_source.get(link, Main.parse_collection)

    # noinspection PyUnusedLocal
    async def shuffle_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        if not state.collection:
            await update.message.reply_text(_('Choose collection first!'))
            return
        state.shuffle_entries()
        await update.message.reply_text(_('Shuffled'))

    @staticmethod
    async def error(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if isinstance(context.error, CapacityException):
            await update.message.reply_text(_('The bot is busy'))
        else:
            logger.error('Update %s caused an error', update, exc_info=context.error)

    @staticmethod
    def parse_collection(path: pathlib.Path, link: GitFileLink) -> Collection:
        content = []
        with open(path.joinpath('entries.csv'), encoding='utf-8') as csv_file:
            for row in csv.reader(csv_file, delimiter=';'):
                content.append(tuple(row))
        with open(path.joinpath('description.yaml'), encoding='UTF-8') as yaml_file:
            descr: dict = yaml.safe_load(yaml_file)
            return Collection(tuple(content), descr['nativeLang'], descr['studiedLang'], descr['topic'], link)

    # noinspection PyUnusedLocal
    def on_refresh(self, url: str, branch: str) -> None:
        for username in self.user_states:
            state: UserState = self.user_states.get(username)
            if not state.collection:
                continue
            link: GitFileLink = state.collection.link
            if (url, branch) != (link.url, link.branch):
                continue
            try:
                state.collection = self.git_source.get(link, Main.parse_collection)
                self.app.job_queue.run_once(
                    lambda ignore: self.app.bot.send_message(
                        state.chat_id,
                        _('Word collection has been modified externally!')
                    ),
                    0
                )
            except FileNotFoundError as e:
                logger.error('Failed to refresh collection %s', link, exc_info=e)

    async def remove_chat_buttons(self, chat_id: int, msg_text: str = 'You are not supposed to see this'):
        msg = await self.app.bot.send_message(chat_id, msg_text, reply_markup=ReplyKeyboardRemove())
        await msg.delete()


if __name__ == '__main__':
    with open('deltabanana.yaml', encoding='UTF-8') as config_file:
        config: dict = yaml.safe_load(config_file)
        _ = gettext.translation('deltabanana', './locales', fallback=False, languages=[config.get('locale', 'en')]).gettext
        logger.info('Starting bot...')
        Main(os.getenv('DELTABANANA_TOKEN'))
