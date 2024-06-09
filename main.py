import asyncio
import csv
import datetime
import gettext
import json
import logging.config
import os.path
import pathlib
from dataclasses import dataclass
from datetime import timedelta, datetime
from random import shuffle
from typing import List, Tuple, Any
import telegram.ext.filters as filters

import yaml
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, \
    ReplyKeyboardRemove
from telegram.error import BadRequest
from telegram.ext import ContextTypes, Application, CommandHandler, JobQueue, CallbackQueryHandler, CallbackContext, \
    MessageHandler

import cfg
from caches import LimitedTtlCache, CapacityException
from gitsource import GitSource, GitFileLink
from timeinterval import TimeInterval

from typing import Final

logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


def to_git_file_link(c: cfg.Collection):
    return GitFileLink(c.url, c.branch, c.path)


NEXT_BUTTON: Final[ReplyKeyboardMarkup] = ReplyKeyboardMarkup([[KeyboardButton('/next')]], resize_keyboard=True)


@dataclass(frozen=True)
class Collection:
    entries: Tuple[Tuple[str]]
    native_lang: str
    studied_lang: str
    topic: str
    link: GitFileLink

    @property
    def title(self) -> str:
        for idx, c in enumerate(config.collections):
            if to_git_file_link(c) == self.link:
                return c.title
        raise IndexError()

    @property
    def decorated_title(self) -> str:
        return f'{self.title} {self.native_lang} {self.studied_lang}'


class UserState:
    __collection: Collection | None
    __mutable_entries: List[Tuple[str]]
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
    def current_word(self) -> str:
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
        text: str = self.current_word.strip()
        while not text or stick_to_question and self.__tuple_idx_with_mode_applied > 0:
            self.go_next_word()
            text = self.current_word.strip()

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
        question_answer_icon = ['❓', '❗', ''][self.__tuple_idx]
        return f'{native_studied_pronunciation_icon} {self.current_word.strip()} {question_answer_icon}'

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


class Main:
    user_states: LimitedTtlCache[str, UserState]
    git_source: GitSource
    app: Application

    def __init__(self, bot_token: str):
        self.user_states = LimitedTtlCache(
            maxsize=config.active_user_sessions.max_count,
            ttl=config.active_user_sessions.inactivity_timeout_seconds
        )
        app = Application.builder().token(bot_token).job_queue(JobQueue()).build()
        app.add_handlers([
            MessageHandler(None, self.interaction_callback),
            CallbackQueryHandler(self.interaction_callback)
        ], 1)
        app.add_handlers([
            CommandHandler('start', self.start_command),
            CommandHandler('next', self.next_command),
            CommandHandler('shuffle', self.shuffle_command),
            CommandHandler('reverse', self.reverse_command),
            CommandHandler('nudge', self.nudge_command),
            CommandHandler('info', self.info_command),
            MessageHandler(filters.TEXT & (~ filters.COMMAND), self.non_command_text),
            CallbackQueryHandler(self.inline_keyboard_button_handler)
        ])
        app.add_error_handler(self.error)
        self.git_source = GitSource(
            refresh_callback=self.on_refresh,
            apply_changes_callback=lambda a, b: None,  # TODO
            sync_interval_seconds=config.collection_sync.interval_seconds,
            no_change_sync_interval_multiplier=config.collection_sync.no_change_multiplier,
            commit_message=config.collection_sync.commit_message
        )
        self.app = app
        app.job_queue.run_repeating(
            callback=self.nudge_users,
            interval=timedelta(seconds=config.nudge.job_interval_seconds)
        )
        app.run_polling(poll_interval=config.bot_poll_interval_seconds)

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
        collections_keyboard: List[List[InlineKeyboardButton]] = []
        for idx, ignore in enumerate(config.collections):
            try:
                data = json.dumps({'type': 'collection_idx', 'value': idx})
                button_markup = [InlineKeyboardButton(self.get_collection(idx).decorated_title, callback_data=data)]
                collections_keyboard.append(button_markup)
            except FileNotFoundError:
                logger.error(f'Failed to load collection #{idx} from config file', exc_info=True)
        reply_markup = InlineKeyboardMarkup(collections_keyboard)
        await asyncio.gather(
            self.remove_chat_buttons(update.effective_chat.id),
            update.message.reply_text(_('collections'), reply_markup=reply_markup)
        )

    # noinspection PyUnusedLocal
    async def next_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        await self.show_next_command(state)

    # noinspection PyUnusedLocal
    async def shuffle_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        if not state.collection:
            await update.message.reply_text(_('select_collection'))
            return
        state.shuffle_entries()
        await update.message.reply_text(_('shuffled'))

    # noinspection PyUnusedLocal
    async def reverse_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        if not state.collection:
            await update.message.reply_text(_('select_collection'))
            return
        state.toggle_reverse_mode()
        lang: str = state.collection.native_lang if state.reverse_mode else state.collection.studied_lang
        await update.message.reply_text(_('reverse_mode_toggle').format(lang=lang))

    # noinspection PyUnusedLocal
    async def nudge_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        if state.nudge_menu_msg:
            await state.delete_nudge_menu()
        button = InlineKeyboardButton(_('nudge_button_reset'), callback_data=Main.nudge_req('RESET')) \
            if state.nudge_is_set else InlineKeyboardButton(_('nudge_button_set'), callback_data=Main.nudge_req('SET'))
        reply_markup = InlineKeyboardMarkup([
            [button],
            [InlineKeyboardButton(_('nudge_button_help'), callback_data=Main.nudge_req('HELP'))]
        ])
        state.nudge_menu_msg = await update.message.reply_text(_('nudge_title'), reply_markup=reply_markup)

    # noinspection PyUnusedLocal
    async def info_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        collection: Collection = state.collection
        if not collection:
            await update.message.reply_text(_('select_collection'))
            return
        await update.message.reply_text(
            _('collection_info').format(
                title=collection.title,
                topic=collection.topic,
                native_lang=collection.native_lang,
                studied_lang=collection.studied_lang,
                url=collection.link.url,
                branch=collection.link.branch,
                path=collection.link.path,
                entry_count=len(collection.entries)
            ),
            parse_mode='html'
        )

    # noinspection PyUnusedLocal
    async def inline_keyboard_button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        data: dict = json.loads(update.callback_query.data)
        data_type: str = data['type']
        value: Any = data['value']
        if data_type == 'collection_idx':
            collection = self.get_collection(int(value))
            state.collection = collection
            await asyncio.gather(
                update.callback_query.answer(),
                update.effective_message.reply_text(
                    _('selected_collection').format(title=collection.decorated_title, topic=collection.topic),
                    parse_mode='html',
                    reply_markup=NEXT_BUTTON
                )
            )
            await self.show_next_command(state)
        elif data_type == 'nudge_request':
            await update.callback_query.answer()
            if value == 'SET':
                state.set_nudge()
                await asyncio.gather(
                    state.delete_nudge_menu(),
                    self.app.bot.send_message(state.chat_id, _('nudge_activated'))
                )
                if not state.collection:
                    await self.app.bot.send_message(state.chat_id, _('nudge_remember_select_collection'))
            elif value == 'RESET':
                state.reset_nudge()
                await asyncio.gather(
                    state.delete_nudge_menu(),
                    self.app.bot.send_message(state.chat_id, _('nudge_deactivated'))
                )
            else:
                hours: int = round(config.nudge.active_interval_seconds / 3600)
                await self.app.bot.send_message(state.chat_id, _('nudge_help_text').format(hours=hours))

    # noinspection PyUnusedLocal
    async def non_command_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        if not state.collection:
            await update.message.reply_text(_('select_collection'))
            return
        lines = update.message.text.splitlines()
        if len(lines) >= 2:
            self.add_entry(state, *lines)
            await update.message.reply_text(_('entry_added'), reply_markup=NEXT_BUTTON)
        else:
            await update.message.reply_text(_('how_to_add_entry'), reply_markup=NEXT_BUTTON)

    # noinspection PyUnusedLocal
    async def interaction_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.user_state(update).update_last_interaction_time()

    @staticmethod
    async def error(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if isinstance(context.error, CapacityException):
            await update.message.reply_text(_('bot_busy'))
        else:
            logger.error(f'Update {update} caused an error {context.error}', exc_info=True)

    async def show_next_command(self, state: UserState, stick_to_questions: bool = False):
        chat_id: int = state.chat_id
        if not state.collection:
            await self.app.bot.send_message(chat_id, _('select_collection'))
            return
        if not state.has_entries:
            await self.app.bot.send_message(chat_id, _('empty_collection'))
            return
        state.roll(stick_to_questions)
        await self.app.bot.send_message(chat_id, state.decorated_word)
        state.go_next_word()

    # noinspection PyUnusedLocal
    async def nudge_users(self, ctx: CallbackContext):
        for username in self.user_states:
            state: UserState = self.user_states.get(username)
            if not state.collection or not state.idling or not state.is_nudge_time:
                continue
            state.update_last_interaction_time()
            ctx.job_queue.run_once(lambda ignore: self.show_next_command(state, True), 0)

    def get_collection(self, idx) -> Collection:
        return self.git_source.get(to_git_file_link(config.collections[idx]), Main.parse_collection)

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
                        _('collection_modified_externally')
                    ),
                    0
                )
            except FileNotFoundError:
                logger.error(f'Failed to refresh collection {link}', exc_info=True)

    async def remove_chat_buttons(self, chat_id: int, msg_text: str = '👻'):
        msg = await self.app.bot.send_message(chat_id, msg_text, reply_markup=ReplyKeyboardRemove())
        await msg.delete()

    @staticmethod
    def nudge_req(value: str) -> str:
        return json.dumps({'type': 'nudge_request', 'value': value})

    def add_entry(self, state: UserState, studied: str, native: str, pronunciation: str = None) -> None:
        self.git_source.register_change(state.collection.link, (studied, native, pronunciation))


if __name__ == '__main__':
    with open('deltabanana.yaml', encoding='UTF-8') as config_file:
        config: cfg.Config = cfg.load(config_file)
        gettext.translation(
            'deltabanana',
            './locales',
            fallback=False,
            languages=[config.locale]
        ).install(['gettext', 'ngettext'])
        logger.info('Starting bot...')
        Main(os.getenv('DELTABANANA_TOKEN'))
