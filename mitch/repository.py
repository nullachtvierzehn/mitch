from functools import cached_property
from pathlib import Path
from typing import Dict, Generator, Iterable, Optional, Self
from graphlib import TopologicalSorter
import tomllib
import pdb

from .utils import CompositeId


class Repository:
    root_folder: Path
    name: str
    is_root: bool = False
    _instantiated: bool = False
    _migrations: Dict[CompositeId, "Migration"]
    _repositories_by_path: Dict[Path, Self] = {}

    def __new__(cls, root_folder: Path = Path.cwd()) -> Self:
        root_folder = root_folder.resolve()
        try:
            return cls._repositories_by_path[root_folder]
        except KeyError:
            inst = super().__new__(cls)
            cls._repositories_by_path[root_folder] = inst
            inst._instantiated = False
            return inst

    @classmethod
    def from_closest_parent(cls, directory: Path = Path.cwd()) -> Self:
        if not directory.is_dir():
            raise NotADirectoryError(f"{directory} is not a directory")

        # Search in parents
        self_and_parents = [*directory.parents, directory]
        for directory in reversed(self_and_parents):
            if (config_file := directory / "mitch.toml").is_file():
                config = tomllib.load(config_file.open(mode="rb"))
                if "repository" in config:
                    return cls(root_folder=directory)
        else:
            raise FileNotFoundError(
                f"Found no mitch.toml with a repository section in {directory} or its parents."
            )

    def __repr__(self) -> str:
        return f"Repository(name={self.name}, root_folder={self.root_folder}, is_root={self.is_root})"

    def __str__(self) -> str:
        return self.__repr__()

    def __init__(self, root_folder: Path = Path.cwd()) -> None:
        if self._instantiated:
            return
        else:
            self._instantiated = True

        # Check that root_folder is a directory.
        if not root_folder.is_dir():
            raise NotADirectoryError(f"{root_folder} is not a directory")

        # Load config from mitch.toml.
        if not (config_file := root_folder / "mitch.toml").is_file():
            raise FileNotFoundError(f"No mitch.toml file found in {root_folder}")

        with config_file.open(mode="rb") as config_file:
            config = tomllib.load(config_file)
            # Get repository section.
            try:
                repository = config["repository"]
            except KeyError:
                raise ValueError(f"Missing repository section in {config_file}")

            # Get name
            try:
                name = repository["name"]
            except KeyError:
                raise ValueError(f"Missing name in repository section of {config_file}")

            is_root = repository.get("root", False)

        # Read repository name.
        self.root_folder = root_folder
        self.name = name
        self.is_root = is_root
        super().__init__()

        # Load migrations to all related repositories.
        for r in [self.root, *self.root.subrepositories.values()]:
            r._load_migrations()
        for r in [self.root, *self.root.subrepositories.values()]:
            r._connect_dependencies()

    @cached_property
    def subrepositories(self) -> Dict[str, Self]:
        # Fetch configs from disk
        _subrepositories: Dict[str, Self] = {}
        for config_path in self.root_folder.glob("*/**/mitch.toml"):
            if self.root_folder.is_relative_to(config_path.parent):
                raise ValueError(f"{config_path} is outside of {self.root_folder}")
            subrepository = self.__class__(config_path.parent)
            _subrepositories[subrepository.name] = subrepository
        return _subrepositories

    @cached_property
    def parent(self) -> Optional[Self]:
        if self.is_root:
            return None
        try:
            return self.__class__.from_closest_parent(directory=self.root_folder.parent)
        except FileNotFoundError:
            return None

    @cached_property
    def root(self) -> Self:
        root = self
        while root.parent:
            root = root.parent
        return root

    def _discover_migrations(self, directory: Path) -> Generator[Path, None, None]:
        # Is this a migration directory?
        if (migration := directory / "migration.toml").is_file():
            yield migration

        for child in directory.iterdir():
            # Skip non-directories
            if not child.is_dir():
                continue
            # Skip directories that belong to sub-repositories
            if (child / "mitch.toml").is_file():
                continue
            # Recurse
            yield from self._discover_migrations(child)

    def _load_migrations(self) -> Dict[CompositeId, "Migration"]:
        try:
            self._migrations.clear()
        except AttributeError:
            self._migrations = {}
        for config_path in self._discover_migrations(self.root_folder):
            migration = Migration.from_config(config_path, repository=self)
            if migration.id in self._migrations:
                raise ValueError(f"Duplicate migration id {migration.id}")
            self._migrations[migration.id] = migration
        return self._migrations

    def _connect_dependencies(self) -> None:
        # Re-initialize dependencies, if empty
        if not self._migrations:
            self._load_migrations()

        # Connect dependencies
        for migration in self._migrations.values():
            for dependency_name in migration.dependencies:
                # Resolve relative dependency names
                if dependency_name.startswith("."):
                    dependency_name = str(
                        migration.directory.joinpath(dependency_name).relative_to(
                            self.root_folder
                        )
                    )

                # Connect dependency
                dependency_name = self._normalize_id(dependency_name)
                try:
                    dependency = self.by_id(dependency_name)
                except KeyError:
                    raise ValueError(f"Unknown dependency {dependency_name}")
                else:
                    migration.resolved_dependencies.add(dependency)
                    dependency.resolved_dependants.add(migration)

    @property
    def migrations(self) -> Dict[CompositeId, "Migration"]:
        try:
            return self._migrations
        except AttributeError:
            self._load_migrations()
            return self._migrations

    def dependencies_of(
        self, migrations: Iterable["Migration"]
    ) -> Generator["Migration", None, None]:
        sorter = TopologicalSorter[Migration]()
        for m in migrations:
            sorter.add(m, *m.resolved_dependencies)
            for d in m.recursive_dependencies:
                sorter.add(d, *d.resolved_dependencies)
        sorter.prepare()
        while sorter.is_active():
            nodes = sorter.get_ready()
            # Sort by creation date, then by id, so that the order is deterministic.
            # In case of unknown creation date, we use datetime.min to sort the migrations at the beginning of the list.
            yield from sorted(nodes, key=lambda m: m.sort_key)
            sorter.done(*nodes)

    def dependants_of(
        self, migrations: Iterable["Migration"]
    ) -> Generator["Migration", None, None]:
        sorter = TopologicalSorter[Migration]()
        for m in migrations:
            sorter.add(m, *m.resolved_dependants)
            for d in m.recursive_dependants:
                sorter.add(d, *d.resolved_dependants)
        sorter.prepare()
        while sorter.is_active():
            nodes = sorter.get_ready()
            # Sort by creation date, then by id, so that the order is deterministic.
            # In case of unknown creation date, we use datetime.min to sort the migrations at the beginning of the list.
            yield from sorted(nodes, key=lambda m: m.sort_key, reverse=True)
            sorter.done(*nodes)

    def by_ids(
        self, ids: Iterable[str | tuple[str, str]]
    ) -> Generator["Migration", None, None]:
        for id in ids:
            yield self.by_id(id)

    def _normalize_id(self, id: str | tuple[str, str]) -> CompositeId:
        return CompositeId.from_string_or_tuple(id, self.name)

    def by_id(self, id: str | tuple[str, str]) -> "Migration":
        normalized_id = self._normalize_id(id)
        if self.name == normalized_id.repository_id:
            repository = self
        else:
            try:
                repository = self.root.subrepositories[normalized_id.repository_id]
            except KeyError as e:
                raise KeyError(
                    f"Unknown repository {normalized_id.repository_id}"
                ) from e
        try:
            return repository.migrations[normalized_id]
        except KeyError as e:
            raise KeyError(f"Unknown migration {id}") from e

    def with_migrations(
        self, applications: Iterable["MigrationApplication"]
    ) -> Generator[tuple["MigrationApplication", Optional["Migration"]], None, None]:
        for a in applications:
            try:
                yield a, self.by_id(a.id)
            except KeyError:
                yield a, None


from .migration import Migration, MigrationApplication
