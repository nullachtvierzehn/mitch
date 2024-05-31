from pathlib import Path
from typing import Dict, Generator, Iterable, Optional, Self
from graphlib import TopologicalSorter    


class Repository:
    root_folder: Path
    available_migrations: Dict[str, 'Migration'] 

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
        if not root_folder.is_dir():
            raise NotADirectoryError(f"{root_folder} is not a directory")
        super().__init__()
        self.root_folder = root_folder
        self.migrations = dict()
        self._load_available_migrations()
    
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

    def _load_available_migrations(self) -> None:
        self.migrations.clear()

        # Fetch configs from disk
        for config_path in self._discover_migrations(self.root_folder):
            migration = Migration.from_config(config_path, repository=self)
            self.migrations[migration.id] = migration
        
        # Connect dependencies
        for migration in self.migrations.values():
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
                    dependency = self.migrations[dependency_name]
                except KeyError:
                    raise ValueError(f"Unknown dependency {dependency_name}")
                else:
                    migration.resolved_dependencies.add(dependency)
                    dependency.resolved_dependants.add(migration)
    
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
        

    def refresh(self):
        self._load_available_migrations()


from .migration import Migration, MigrationApplication