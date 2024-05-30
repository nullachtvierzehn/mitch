from typing import Dict, Generator, Iterable, Optional
from psycopg import Connection
from psycopg.rows import class_row
import re

import click

from .migration import MigrationApplication, Migration


multiple_spaces = re.compile(r"\s+", re.MULTILINE)


class AbstractTarget:
    pass


class PostgreSqlTarget(AbstractTarget):
    connection: Connection
    applications: Dict[str, MigrationApplication]

    def __init__(self, connection: Connection):
        self.connection = connection
        self.connection.autocommit = False
        self.install_or_update_mitch_in_database()
        self.applications = dict()
        self._load_applications()
        super().__init__()
    
    def transaction(self):
        return self.connection.transaction()

    def install_or_update_mitch_in_database(self):
        """
        Install schema if it doesn't exist.
        It stores information about which migrations have been applied.
        """
        with self.transaction():
            self.connection.execute(
                """
                create schema if not exists mitch;

                create table if not exists mitch.applied_migrations (
                    migration_id text primary key,
                    up_script_sha256 char(64) not null,
                    reformatted_up_script_sha256 char(64),
                    is_dependency boolean not null default false,
                    applied_at timestamptz not null default now(),
                    applied_by name not null default current_user
                );
                """
            )


    def _load_applications(self):
        self.applications.clear()
        with self.transaction():
            cur = self.connection.cursor(row_factory=class_row(MigrationApplication))
            cur.execute("select * from mitch.applied_migrations order by applied_at asc")
            for row in cur.fetchall():
                #assert row.id in available_migrations, f"Missing migration ${row.id}"
                #applied_migration_ids.add(row.id)
                #row.migration_on_disk = available_migrations[row.id]
                self.applications[row.migration_id] = row
    
    def with_applications(self, migrations: Iterable[Migration]) -> Generator[tuple[Migration, Optional[MigrationApplication]], None, None]:
        for m in migrations:
            yield m, self.applications.get(m.id)
    

    def up(self, migration: Migration, as_dependency: bool):
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
                insert into mitch.applied_migrations 
                    (migration_id, is_dependency, up_script_sha256, reformatted_up_script_sha256) 
                values (%s, %s, %s, %s)
                on conflict (migration_id) do update set
                    is_dependency = excluded.is_dependency,
                    up_script_sha256 = excluded.up_script_sha256,
                    reformatted_up_script_sha256 = excluded.reformatted_up_script_sha256,
                    applied_at = excluded.applied_at,
                    applied_by = excluded.applied_by
                """,
                (
                    migration.id,
                    as_dependency,
                    migration.up_script_sha256,
                    migration.reformatted_up_script_sha256,
                ),
            )
    
    def down(self, migration: Migration):
        with self.connection.cursor() as cur:
            click.echo(f"Revert migration {migration.id}")
            for cmd in migration.commands_of_down_script:
                click.echo(f"- {multiple_spaces.sub(" ", cmd)} ", nl=False)
                cur.execute(cmd.encode('utf-8'))
                click.echo("[ ok ]")
            cur.execute("delete from mitch.applied_migrations where migration_id = %s", (migration.id,))
    
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
                    and (
                        up_script_sha256 is distinct from %(up_script_sha256)s
                        or reformatted_up_script_sha256 is distinct from %(reformatted_up_script_sha256)s
                        or is_dependency is distinct from %(is_dependency)s
                    )
                """,
                dict(
                    is_dependency=is_dependency,
                    up_script_sha256=migration.up_script_sha256,
                    reformatted_up_script_sha256=migration.reformatted_up_script_sha256,
                    migration_id=migration.id
                )
            )


    def refresh(self):
        self._load_applications()