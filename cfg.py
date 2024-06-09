import yaml
from dacite import from_dict
from dataclasses import dataclass, field

from gitsource import GitFileLink


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
class Collection:
    title: str
    url: str
    path: str
    branch: str = field(default='main')

    @property
    def as_git_file_link(self):
        return GitFileLink(self.url, self.branch, self.path)


@dataclass(frozen=True)
class Config:
    locale: str = field(default='en')
    collection_sync: CollectionSync = field(default_factory=CollectionSync)
    bot_poll_interval_seconds: int = field(default=2)
    active_user_sessions: ActiveUserSessions = field(default_factory=ActiveUserSessions)
    nudge: Nudge = field(default_factory=Nudge)
    collections: list[Collection] = field(default_factory=list)


def load(file) -> Config:
    return from_dict(Config, yaml.safe_load(file))


with open('deltabanana.yaml', encoding='UTF-8') as config_file:
    config: Config = load(config_file)
