from typing import Dict, Generator, Iterable, Optional, Collection
from psycopg import Connection
from functools import cached_property
import re

from psycopg.rows import class_row
import click

from mitch.repository import Repository

from .migration import MigrationApplication, Migration
from .utils import CompositeId


multiple_spaces = re.compile(r"\s+", re.MULTILINE)


class AbstractTarget:
    pass


class PostgreSqlTarget(AbstractTarget):
    connection: Connection

    def __init__(self, connection: Connection):
        self.connection = connection
        self.connection.autocommit = False
        self.install_or_update_mitch_in_database()
        super().__init__()
    
    def transaction(self, force_rollback: bool = False):
        return self.connection.transaction(force_rollback=force_rollback)

    def install_or_update_mitch_in_database(self):
        """
        Install schema if it doesn't exist.
        It stores information about which migrations have been applied.
        """
        with self.transaction():
            self.connection.execute(
                """
                create schema if not exists mitch;

                create table if not exists mitch.repositories (
                    repository_id text primary key
                );

                create table if not exists mitch.applied_migrations (
                    repository_id text not null references mitch.repositories(repository_id) on update cascade on delete restrict,
                    migration_id text not null,
                    constraint applied_migrations_pk 
                        primary key (migration_id, repository_id),
                    up_script_sha256 char(64) not null,
                    reformatted_up_script_sha256 char(64),
                    is_dependency boolean not null default false,
                    applied_at timestamptz not null default statement_timestamp(),
                    applied_by name not null default current_user
                );

                create index if not exists applied_migrations_on_applied_at on mitch.applied_migrations using btree (applied_at);
                create index if not exists applied_migrations_on_up_script_sha256 on mitch.applied_migrations using hash (up_script_sha256);
                create index if not exists applied_migrations_on_reformatted_up_script_sha256 on mitch.applied_migrations using hash (reformatted_up_script_sha256);
                create index if not exists applied_migrations_on_composite_id on mitch.applied_migrations using btree ((repository_id || ':' || migration_id));
                """
            )

    @cached_property
    def applications(self) -> Dict[CompositeId, MigrationApplication]:
        with self.transaction():
            cur = self.connection.cursor(row_factory=class_row(MigrationApplication))
            cur.execute("select * from mitch.applied_migrations order by applied_at")
            return {CompositeId(row.repository_id, row.migration_id): row for row in cur.fetchall()}

    def with_applications(self, migrations: Iterable[Migration]) -> Generator[tuple[Migration, Optional[MigrationApplication]], None, None]:
        for m in migrations:
            yield m, self.applications.get(m.id)
    
    def installed_migrations(self, repository: Repository, include_dependencies: bool = False) -> Generator[Migration, None, None]:
        for a, m in repository.with_migrations(self.applications.values()):
            if m and (not a.is_dependency or include_dependencies):
                yield m
    
    def modified_migrations(self, repository: Repository) -> Generator[Migration, None, None]:
        for a, m in repository.with_migrations(self.applications.values()):
            if m and not a.matches(m):
                yield m

    def prune(self, repository: Repository, except_migrations: Collection[Migration] = ()) -> None:
        installed_migrations = set(self.installed_migrations(repository, include_dependencies=True))
        needed_migrations = set(
            except_migrations 
            if len(except_migrations) > 0 
            else self.installed_migrations(repository, include_dependencies=False)
        )
        dangling_migrations = installed_migrations - needed_migrations
        
        with self.transaction():
            # Execute migrations in topological order
            for m in reversed(list(
                m for m in repository.dependencies_of(dangling_migrations) 
                if m in dangling_migrations
            )):
                self.down(m)


    def up(self, *migrations: Migration, as_dependency: bool):
        for migration in migrations:
            click.echo(f"Run migration {migration.id}")
            with self.connection.cursor() as cur:
                # Run up script, command by command.
                for cmd in migration.commands_of_up_script:
                    click.echo(f"- {multiple_spaces.sub(" ", cmd)} ", nl=False)
                    cur.execute(cmd.encode('utf-8'))
                    click.echo("[ ok ]")

                # Mark migration as applied.
                cur.execute(
                    """
                    insert into mitch.repositories (repository_id)
                    values (%s)
                    on conflict (repository_id) do nothing;
                    """,
                    (migration.repository.name,)
                )

                cur.execute(
                    """
                    insert into mitch.applied_migrations 
                        (repository_id, migration_id, is_dependency, up_script_sha256, reformatted_up_script_sha256) 
                    values (%s, %s, %s, %s, %s)
                    on conflict (repository_id, migration_id) do update set
                        is_dependency = excluded.is_dependency,
                        up_script_sha256 = excluded.up_script_sha256,
                        reformatted_up_script_sha256 = excluded.reformatted_up_script_sha256,
                        applied_at = excluded.applied_at,
                        applied_by = excluded.applied_by
                    """,
                    (
                        migration.repository.name,
                        migration.migration_id,
                        as_dependency,
                        migration.up_script_sha256,
                        migration.reformatted_up_script_sha256,
                    ),
                )
        try:
            del self.applications
        except AttributeError:
            pass  # ignore repeated deletions without prior re-computations
    
    def down(self, *migrations: Migration):
        with self.connection.cursor() as cur:
            for migration in migrations:
                click.echo(f"Revert migration {migration.id}")
                for cmd in migration.commands_of_down_script:
                    click.echo(f"- {multiple_spaces.sub(" ", cmd)} ", nl=False)
                    cur.execute(cmd.encode('utf-8'))
                    click.echo("[ ok ]")
                cur.execute("delete from mitch.applied_migrations where repository_id = %s and migration_id = %s", migration.id)
        try:
            del self.applications
        except AttributeError:
            pass  # ignore repeated deletions without prior re-computations
    
    def fix_hashes_and_status(self, migration: Migration, is_dependency: bool):
        with self.connection.cursor() as cur:
            cur.execute(
                """
                update mitch.applied_migrations set
                    is_dependency = %(is_dependency)s,
                    up_script_sha256 = %(up_script_sha256)s,
                    reformatted_up_script_sha256 = %(reformatted_up_script_sha256)s
                where 
                    migration_id = %(migration_id)s
                    and repository_id = %(repository_id)s
                    and (
                        up_script_sha256 is distinct from %(up_script_sha256)s
                        or reformatted_up_script_sha256 is distinct from %(reformatted_up_script_sha256)s
                        or is_dependency is distinct from %(is_dependency)s
                    )
                """,
                dict(
                    repository_id=migration.repository.name,
                    is_dependency=is_dependency,
                    up_script_sha256=migration.up_script_sha256,
                    reformatted_up_script_sha256=migration.reformatted_up_script_sha256,
                    migration_id=migration.migration_id
                )
            )
        del self.applications
