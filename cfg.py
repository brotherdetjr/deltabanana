import os
from dataclasses import dataclass, field
from typing import TypeVar

import yaml
from dacite import from_dict

from gitsource import GitFileLink

T = TypeVar("T")


@dataclass(frozen=True)
class CollectionSync:
    interval_seconds: int = field(default=60)
    no_change_multiplier: int = field(default=10)
    commit_message: str = field(default='Commit by deltabanana bot')


@dataclass(frozen=True)
class ActiveUserSessions:
    max_count: int = field(default=1000)
    inactivity_timeout_seconds: int = field(default=604800)


@dataclass(frozen=True)
class Nudge:
    job_interval_seconds: int = field(default=300)
    active_interval_seconds: int = field(default=43200)
    idling_interval_seconds: int = field(default=7200)


@dataclass(frozen=True)
class Config:
    persisted_config_link: GitFileLink
    bot_token: str = field(default=os.getenv('DELTABANANA_TOKEN'))
    locale: str = field(default='en')
    collection_sync: CollectionSync = field(default_factory=CollectionSync)
    bot_poll_interval_seconds: int = field(default=2)
    active_user_sessions: ActiveUserSessions = field(default_factory=ActiveUserSessions)
    nudge: Nudge = field(default_factory=Nudge)
    admin: str = field(default='')


# noinspection PyDataclass
@dataclass(frozen=True, kw_only=True)
class CollectionDescriptor(GitFileLink):
    title: str
    restricted: bool = field(default=True)


@dataclass(frozen=True)
class PersistedConfig:
    collections: list[CollectionDescriptor]


def load(file, data_class: type[T]) -> T:
    return from_dict(data_class, yaml.safe_load(file))


with open('deltabanana.yaml', encoding='UTF-8') as config_file:
    config: Config = load(config_file, Config)
