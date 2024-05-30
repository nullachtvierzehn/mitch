from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Set, List, Self
from datetime import datetime
from hashlib import sha256
import tomllib

import sqlparse

@dataclass
class Migration:
    directory: Path
    id: str
    repository: 'Repository'
    author: Optional[str] = None
    created_at: Optional[datetime] = None
    dependencies: Set[str] = field(default_factory=set)
    resolved_dependencies: Set[Self] = field(
        default_factory=set, init=False
    )
    resolved_dependants: Set[Self] = field(
        default_factory=set, init=False
    )
    idempotent: bool = False
    transactional: bool = True

    def __hash__(self) -> int:
        return hash(self.id)

    @property
    def sort_key(self) -> tuple[datetime, str]:
        """
        Sort by creation date, then by id, so that the order is deterministic.
        In case of unknown creation date, we use datetime.min to sort the migrations at the beginning of the list.
        """
        return (self.created_at or datetime.min, self.id)

    @classmethod
    def from_config(cls, config_path: Path, repository: 'Repository') -> Self:
        config = tomllib.loads(config_path.read_text("utf-8"))
        config.setdefault("id", str(config_path.parent.relative_to(repository.root_folder)))
        relations = config.pop("relations", {})
        dependencies = relations.get("dependencies") or config.pop("dependencies") or set()
        return cls(
            directory=config_path.parent, 
            dependencies=dependencies, 
            repository=repository,
            **config
        )

    @property
    def recursive_dependencies(self) -> Set[Self]:
        out = set(self.resolved_dependencies)
        for dep in self.resolved_dependencies:
            out |= dep.recursive_dependencies
        return out

    @property
    def recursive_dependants(self) -> Set[Self]:
        out = set(self.resolved_dependants)
        for dep in self.resolved_dependants:
            out |= dep.recursive_dependants
        return out

    @property
    def up_script(self) -> str:
        return self.directory.joinpath("up.sql").read_text("utf-8")

    @property
    def reformatted_up_script(self) -> str:
        return "\n\n".join(
            sqlparse.format(
                cmd,
                keyword_case="lower",
                identifier_case="lower",
                strip_comments=True,
                reindent=True,
                reindent_aligned=True,
                use_space_around_operators=True,
                indent_tabs=False,
                indent_width=2,
                comma_first=True,
            )
            for cmd in self.commands_of_up_script
        )

    @property
    def commands_of_up_script(self) -> List[str]:
        return [
            cmd
            for cmd in sqlparse.split(self.up_script)
            if not (cmd.isspace() or cmd.startswith("--"))
        ]

    @property
    def commands_of_down_script(self) -> List[str]:
        return [
            cmd
            for cmd in sqlparse.split(self.down_script)
            if not (cmd.isspace() or cmd.startswith("--"))
        ]

    @property
    def sha256_of_up_script(self) -> str:
        return sha256(self.up_script.encode("utf-8")).hexdigest()

    @property
    def sha256_of_reformatted_up_script(self) -> str:
        return sha256(self.reformatted_up_script.encode("utf-8")).hexdigest()

    @property
    def down_script(self) -> str:
        return self.directory.joinpath("down.sql").read_text("utf-8")


@dataclass
class MigrationApplication:
    id: str
    sha256_of_up_script: str
    sha256_of_reformatted_up_script: Optional[str] = None
    as_a_dependency: bool = False
    applied_at: datetime = datetime.now()
    applied_by: str = "current_user"

    def matches(self, migration: Migration) -> bool:
        return self.sha256_of_up_script == migration.sha256_of_up_script or self.sha256_of_reformatted_up_script == migration.sha256_of_reformatted_up_script

from .repository import Repository