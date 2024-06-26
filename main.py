import asyncio
import csv
import gettext
import json
import logging.config
import pathlib
from datetime import timedelta
from typing import Any
from typing import Final

import telegram.ext.filters as filters
import yaml
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, \
    ReplyKeyboardRemove
from telegram.ext import ContextTypes, Application, CommandHandler, JobQueue, CallbackQueryHandler, CallbackContext, \
    MessageHandler

from caches import LimitedTtlCache, CapacityException
from cfg import config, PersistedConfig
from gitsource import GitSource, GitFileLink
from state import UserState, Collection, Entry

logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)

NEXT_BUTTON: Final[ReplyKeyboardMarkup] = ReplyKeyboardMarkup([[KeyboardButton('/next')]], resize_keyboard=True)


class Main:
    user_states: LimitedTtlCache[str, UserState]
    git_source: GitSource
    app: Application

    def __init__(self):
        self.user_states = LimitedTtlCache(
            maxsize=config.active_user_sessions.max_count,
            ttl=config.active_user_sessions.inactivity_timeout_seconds
        )
        app = Application.builder().token(config.bot_token).job_queue(JobQueue()).build()
        app.add_handlers([
            MessageHandler(None, self.interaction_callback),
            CallbackQueryHandler(self.interaction_callback)
        ], 1)
        app.add_handlers([
            CommandHandler('start', self.start_command),
            CommandHandler('next', self.next_command),
            CommandHandler('reverse', self.reverse_command),
            CommandHandler('add', self.add_command),
            CommandHandler('nudge', self.nudge_command),
            CommandHandler('info', self.info_command),
            MessageHandler(filters.TEXT & (~ filters.COMMAND), self.non_command_text),
            CallbackQueryHandler(self.inline_keyboard_button_handler)
        ])
        app.add_error_handler(self.error)
        self.git_source = GitSource(
            refresh_callback=self.on_refresh,
            apply_changes_callback=Main.append_entries_to_file,
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
        collections_keyboard: list[list[InlineKeyboardButton]] = []
        for idx, descriptor in enumerate(self.persisted_config.collections):
            # TODO fully-fledged authorisation check
            if descriptor.restricted:
                continue
            try:
                data = json.dumps({'type': 'collection_idx', 'value': idx})
                button_markup = [InlineKeyboardButton(self.get_collection(idx).decorated_title, callback_data=data)]
                collections_keyboard.append(button_markup)
            except FileNotFoundError:
                logger.error(f'Failed to load collection #{idx} from config file', exc_info=True)
        await asyncio.gather(
            self.remove_chat_buttons(update.effective_chat.id),
            update.message.reply_text(_('collections'), reply_markup=(InlineKeyboardMarkup(collections_keyboard))) \
                if collections_keyboard else update.message.reply_text(
                    _('no_collections').format(
                        admin=' ' + config.admin if config.admin else '',
                        id=update.effective_user.id
                    )
                )
        )

    # noinspection PyUnusedLocal
    async def next_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await asyncio.gather(
            update.message.delete(),
            self.show_next_command(self.user_state(update))
        )

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
    async def add_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        state: UserState = self.user_state(update)
        if not state.collection:
            await update.message.reply_text(_('select_collection'))
            return
        await update.message.reply_text(_('how_to_add_entry'), parse_mode='html', reply_markup=NEXT_BUTTON)

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
        if 1 < len(lines) < 4:
            self.git_source.register_change(state.collection.link, Entry(*lines, author=update.effective_user.username))
            await update.message.reply_text(_('entry_added'), reply_markup=NEXT_BUTTON)
        else:
            await update.message.reply_text(_('how_to_add_entry'), parse_mode='html', reply_markup=NEXT_BUTTON)

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

    @property
    def persisted_config(self) -> PersistedConfig:
        return self.git_source.get(config.persisted_config_link, PersistedConfig)

    def get_collection(self, idx) -> Collection:
        return self.git_source.get(self.persisted_config.collections[idx], self.parse_collection)

    # noinspection PyTypeChecker
    def parse_collection(self, path: pathlib.Path, link: GitFileLink) -> Collection:
        content: list[Entry] = []
        with open(path.joinpath('entries.csv'), encoding='utf-8') as csv_file:
            for row in csv.reader(csv_file, delimiter=';'):
                content.append(Entry(*row))
        with open(path.joinpath('description.yaml'), encoding='UTF-8') as yaml_file:
            descr: dict = yaml.safe_load(yaml_file)
            title: str | None = None
            for ignore, c in enumerate(self.persisted_config.collections):
                if c == link:
                    title = c.title
            if title is None:
                raise IndexError()
            return Collection(tuple(content), descr['nativeLang'], descr['studiedLang'], descr['topic'], link, title)

    # noinspection PyUnusedLocal
    def on_refresh(self, url: str, branch: str) -> None:
        pcl = config.persisted_config_link
        if (url, branch) == (pcl.url, pcl.branch):
            return
        for username in self.user_states:
            state: UserState = self.user_states.get(username)
            if not state.collection:
                continue
            link: GitFileLink = state.collection.link
            if (url, branch) != (link.url, link.branch):
                continue
            try:
                state.collection = self.git_source.get(link, self.parse_collection)
                self.app.job_queue.run_once(
                    lambda ignore: self.app.bot.send_message(
                        state.chat_id,
                        _('collection_updated')
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

    @staticmethod
    def append_entries_to_file(entries: list[Entry], link: GitFileLink) -> None:
        path = f'{link.dir_name()}/{link.path}/entries.csv'
        logger.info(f'Appending {entries} to {path}')
        with open(path, 'a', encoding='utf-8') as file:
            if Main.need_to_add_new_line(path):
                file.write('\n')
            csv.writer(file, delimiter=';', lineterminator='\n').writerows(entries)

    @staticmethod
    def need_to_add_new_line(path: str) -> bool:
        with open(path, 'r') as file:
            file.seek(0, 2)
            if file.tell() == 0:
                return False
            file.seek(file.tell() - 1)
            return file.read(1) != '\n'


if __name__ == '__main__':
    gettext.translation('deltabanana', './locales', fallback=False, languages=[config.locale]) \
        .install(['gettext', 'ngettext'])
    logger.info('Starting bot...')
    Main()
