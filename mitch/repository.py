from pathlib import Path
from typing import Dict, Generator, Iterable
from graphlib import TopologicalSorter

from .migration import Migration


class Repository:
    root_folder: Path
    available_migrations: Dict[str, Migration] 

    def __init__(self, root_folder: Path) -> None:
        super().__init__()
        self.root_folder = root_folder
        self.available_migrations = dict()
        self._load_available_migrations()
    
    def _load_available_migrations(self) -> None:
        self.available_migrations.clear()

        # Fetch configs from disk
        for config_path in self.root_folder.glob("**/migration.toml"):
            migration = Migration.from_config(config_path, root=self.root_folder)
            self.available_migrations[migration.id] = migration
        
        # Connect dependencies
        for migration in self.available_migrations.values():
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
                    dependency = self.available_migrations[dependency_name]
                except KeyError:
                    raise ValueError(f"Unknown dependency {dependency_name}")
                else:
                    migration.resolved_dependencies.add(dependency)
                    dependency.resolved_dependants.add(migration)
    
    def dependencies_of(self, *migrations: Migration) -> Generator[Migration, None, None]:
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
    
    def dependants_of(self, *migrations: Migration) -> Generator[Migration, None, None]:
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
    
    def by_ids(self, ids: Iterable[str]) -> Generator[Migration, None, None]:
        for id in ids:
            try:
                yield self.available_migrations[id]
            except KeyError:
                raise ValueError(f"Unknown migration {id}")
    
    def refresh(self):
        self._load_available_migrations()