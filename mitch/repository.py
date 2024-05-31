from functools import cached_property
from pathlib import Path
from typing import Dict, Generator, Iterable, Optional, Self
from graphlib import TopologicalSorter
import tomllib


class Repository:
    root_folder: Path
    config_file: Path
    name: str

    @classmethod
    def from_closest_parent(cls, directory: Path = Path.cwd()) -> Self:
        if not directory.is_dir():
            raise NotADirectoryError(f"{directory} is not a directory")

        # Search in current directory
        if (directory / "mitch.toml").is_file():
            return cls(root_folder=directory)

        # Search in parents
        for directory in reversed(directory.parents):
            if (directory / "mitch.toml").is_file():
                return cls(root_folder=directory)
        else:
            raise FileNotFoundError(f"No mitch.toml file found in parents")

    def __init__(self, root_folder: Path) -> None:
        # Check that root_folder is a directory.
        if not root_folder.is_dir():
            raise NotADirectoryError(f"{root_folder} is not a directory")
        self.root_folder = root_folder
        
        # Load config from mitch.toml.
        if not (config_file := root_folder / "mitch.toml").is_file():
            raise FileNotFoundError(f"No mitch.toml file found in {root_folder}")
        self.config_file = config_file
        config = tomllib.loads(config_file.open(mode="r", encoding="utf-8").read())
        
        # Read repository name.
        if not (name := config.get("repository", {}).get("name")):
            raise ValueError(f"No repository name found in {config_file}")
        self.name = name
        super().__init__()
    
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

    @cached_property
    def migrations(self) -> Dict[str, 'Migration']:
        # Fetch configs from disk
        _migrations = {}
        for config_path in self._discover_migrations(self.root_folder):
            migration = Migration.from_config(config_path, repository=self)
            _migrations[migration.id] = migration
        
        # Connect dependencies
        for migration in _migrations.values():
            for dependency_name in migration.dependencies:
                # Resolve relative dependency names
                if dependency_name.startswith("."):
                    dependency_name = str(
                        migration.directory.joinpath(dependency_name).relative_to(
                            self.root_folder
                        )
                    )

                # Connect dependency
                try:
                    dependency = _migrations[dependency_name]
                except KeyError:
                    raise ValueError(f"Unknown dependency {dependency_name}")
                else:
                    migration.resolved_dependencies.add(dependency)
                    dependency.resolved_dependants.add(migration)
        
        return _migrations
    
    def dependencies_of(self, migrations: Iterable['Migration']) -> Generator['Migration', None, None]:
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
    
    def dependants_of(self, migrations: Iterable['Migration']) -> Generator['Migration', None, None]:
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
    
    def by_ids(self, ids: Iterable[str]) -> Generator['Migration', None, None]:
        for id in ids:
            try:
                yield self.migrations[id]
            except KeyError:
                raise ValueError(f"Unknown migration {id}")
    
    def with_migrations(self, applications: Iterable['MigrationApplication']) -> Generator[tuple['MigrationApplication', Optional['Migration']], None, None]:
        for a in applications:
            yield a, self.migrations.get(a.migration_id)


from .migration import Migration, MigrationApplication