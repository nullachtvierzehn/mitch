#!/usr/bin/env python

import typing
from dataclasses import dataclass, field
from datetime import datetime
from graphlib import TopologicalSorter
from hashlib import sha256
from pathlib import Path
from typing import Optional
import tomllib
import re
import sys

import click
import psycopg
import sqlparse
from psycopg.rows import class_row

from .migration import Migration
from .repository import Repository

multiple_spaces = re.compile(r"\s+", re.MULTILINE)



db = psycopg.connect(autocommit=False)


# Install schema if it doesn't exist.
# It stores information about which migrations have been applied.
with db.transaction(), db.cursor() as cur:
    cur.execute(
        """
        create schema if not exists mitch;

        create table if not exists mitch.applied_migrations (
            id text primary key,
            sha256_of_up_script char(64) not null,
            sha256_of_reformatted_up_script char(64),
            as_a_dependency boolean not null default false,
            applied_at timestamptz not null default now(),
            applied_by name not null default current_user
        );
        """
    )


@dataclass
class AppliedMigration:
    id: str
    sha256_of_up_script: str
    sha256_of_reformatted_up_script: Optional[str] = None
    as_a_dependency: bool = False
    applied_at: datetime = datetime.now()
    applied_by: str = "current_user"
    migration_on_disk: Optional[Migration] = field(init=False)

    @property
    def matching_sha256_of_up_script(self) -> bool | None:
        if not self.migration_on_disk:
            return None
        return self.sha256_of_up_script == self.migration_on_disk.sha256_of_up_script

    @property
    def matching_sha256_of_reformatted_up_script(self) -> bool | None:
        if not self.migration_on_disk:
            return None
        return (
            self.sha256_of_reformatted_up_script
            == self.migration_on_disk.sha256_of_reformatted_up_script
        )

    @property
    def matching_sha256(self) -> bool | None:
        return (
            self.matching_sha256_of_up_script
            or self.matching_sha256_of_reformatted_up_script
        )


available_migrations: typing.Dict[str, Migration] = {}
applied_migration_ids: typing.Set[str] = set()
applied_migrations: typing.Dict[str, AppliedMigration] = {}

def migration_sort_key(migration: Migration) -> tuple[datetime, str]:
    return (migration.created_at or datetime.min, migration.id)


def topologically_sorted_dependencies(
    *migration: Migration,
) -> typing.Generator[Migration, None, None]:
    sorter = TopologicalSorter()
    for m in migration:
        sorter.add(m, *m.resolved_dependencies)
        for d in m.recursive_dependencies:
            sorter.add(d, *d.resolved_dependencies)
    sorter.prepare()
    while sorter.is_active():
        nodes = sorter.get_ready()
        yield from sorted(nodes, key=migration_sort_key)
        sorter.done(*nodes)


def topologically_sorted_dependants(
    *migration: Migration,
) -> typing.Generator[Migration, None, None]:
    sorter = TopologicalSorter()
    for m in migration:
        sorter.add(m, *m.resolved_dependants)
        for d in m.recursive_dependants:
            sorter.add(d, *d.resolved_dependants)
    sorter.prepare()
    while sorter.is_active():
        nodes = sorter.get_ready()
        yield from sorted(nodes, key=migration_sort_key, reverse=True)
        sorter.done(*nodes)


def load_applied_migrations():
    global applied_migration_ids
    global applied_migrations

    # Reset installed migrations
    applied_migration_ids.clear()
    applied_migrations.clear()

    # Read installed migrations from database
    with db.cursor(row_factory=class_row(AppliedMigration)) as cur:
        cur = cur.execute(
            "select * from mitch.applied_migrations order by applied_at asc"
        )
        for row in cur.fetchall():
            assert row.id in available_migrations, f"Missing migration ${row.id}"
            applied_migration_ids.add(row.id)
            row.migration_on_disk = available_migrations[row.id]
            applied_migrations[row.id] = row
        db.commit()


def load_available_migrations(repository: Path):
    global available_migrations

    # Reset available
    available_migrations.clear()

    # Read available migrations from disk
    for config_path in repository.glob("**/migration.toml"):
        migration = Migration.from_config(config_path, root=repository)
        available_migrations[migration.id] = migration

    # Connect dependencies
    for migration in available_migrations.values():
        for dependency_name in migration.dependencies:
            # Resolve relative dependency names
            if dependency_name.startswith("."):
                dependency_name = str(
                    migration.directory.joinpath(dependency_name).relative_to(
                        repository
                    )
                )

            # Connect dependency
            try:
                dependency = available_migrations[dependency_name]
            except KeyError:
                raise ValueError(f"Unknown dependency {dependency_name}")
            else:
                migration.resolved_dependencies.add(dependency)
                dependency.resolved_dependants.add(migration)


@click.group()
@click.pass_context
def cli(ctx):
    load_available_migrations(Path.cwd())
    load_applied_migrations()


@cli.command()
@click.option("--target", "-t", default="default")
@click.option("files", "--from-file", multiple=True, type=click.Path(exists=True, readable=True, file_okay=True, dir_okay=False, allow_dash=True))
@click.option("--save", type=click.Path(writable=True, file_okay=True, dir_okay=False, allow_dash=False), default=None)
@click.option("--as-dependency/--as-explicitely-installed", default=False)
@click.argument("migration", nargs=-1)
@click.pass_context
def apply(ctx, migration: typing.List[str], files: typing.List[str], target: str, as_dependency: bool, save: str | None):
    repository = Repository(root_folder=Path.cwd())

    # Choose migrations
    chosen_migration_ids = set(migration)
    for f in files:
        lines = click.open_file(f, mode="r", encoding="utf-8").readlines()
        chosen_migration_ids.update(l.strip() for l in lines if not l.isspace())
    chosen_migrations = repository.by_ids(chosen_migration_ids) 

    # Execute migrations in topological order
    with db.transaction(), db.cursor() as cur:
        for m in repository.dependencies_of(*chosen_migrations):
            try:
                # Check, if migration has been already applied.
                applied_migration = applied_migrations[m.id]
            except KeyError:
                pass
            else:
                # Check, if migration has been already applied with the same script.
                if applied_migration.matching_sha256:
                    click.echo(f"Migration {m.id} already applied. [ skipped ]")
                    # Fix one of the two hashes, if different from disk.
                    if not applied_migration.matching_sha256_of_up_script:
                        cur.execute("update mitch.applied_migrations set sha256_of_up_script = %s where id = %s", (m.sha256_of_up_script, m.id))
                    elif not applied_migration.matching_sha256_of_reformatted_up_script:
                        cur.execute("update mitch.applied_migrations set sha256_of_reformatted_up_script = %s where id = %s", (m.sha256_of_reformatted_up_script, m.id))
                    
                    # Mark as explicitely installed in the database, if it was the chosen_migration.
                    if applied_migration.as_a_dependency and m.id in chosen_migration_ids:
                        cur.execute("update mitch.applied_migrations set as_a_dependency = false where id = %s", (m.id,))
                        click.echo(f"- marked as explicitely installed")
                    elif not applied_migration.as_a_dependency and as_dependency is True:
                        cur.execute("update mitch.applied_migrations set as_a_dependency = true where id = %s", (m.id,))
                        click.echo(f"- explicitely marked as a dependency")
                        if save:
                            click.open_file(save, mode="a", encoding="utf-8").write(f"{m.id}\n")
                    continue
                elif (
                    applied_migration.migration_on_disk
                    and applied_migration.migration_on_disk.idempotent
                ):
                    if click.confirm(
                        f"Migration {m.id} has been applied with a different script, but is marked as idempotent. Try to reapply?"
                    ):
                        pass
                    else:
                        raise ValueError(
                            f"Migration {m.id} has been applied with a different script"
                        )
                else:
                    raise ValueError(
                        f"Migration {m.id} has been applied with a different script"
                    )

            click.echo(f"Run migration {m.id}")
            for cmd in m.commands_of_up_script:
                # Write a regular expression to replace sequences of whitespaces (that include at least one newline) with a single space.
                click.echo(f"- {multiple_spaces.sub(" ", cmd)} ", nl=False)
                cur.execute(cmd)
                if save:
                    click.open_file(save, mode="a", encoding="utf-8").write(f"{m.id}\n")
                click.echo("[ ok ]")

            cur.execute(
                """
                insert into mitch.applied_migrations 
                  (id, as_a_dependency, sha256_of_up_script, sha256_of_reformatted_up_script) 
                values (%s, %s, %s, %s)
                on conflict (id) do update set
                  as_a_dependency = excluded.as_a_dependency,
                  sha256_of_up_script = excluded.sha256_of_up_script,
                  sha256_of_reformatted_up_script = excluded.sha256_of_reformatted_up_script,
                  applied_at = excluded.applied_at,
                  applied_by = excluded.applied_by
                """,
                (
                    m.id,
                    as_dependency or m.id not in chosen_migration_ids,
                    m.sha256_of_up_script,
                    m.sha256_of_reformatted_up_script,
                ),
            )


@cli.command()
@click.option("--with-dependencies/--without-dependencies", "-d/-D", default=False)
@click.option("--target", "-t", default="default")
@click.argument("migration", nargs=-1)
@click.pass_context
def unapply(ctx, migration: typing.List[str], with_dependencies: bool, target: str):
    # Fetch migration(s)
    repository = Repository(root_folder=Path.cwd())
    chosen_migrations = repository.by_ids(migration)

    # Execute migrations in topological order
    with db.transaction(), db.cursor() as cur:
        for m in repository.dependants_of(*chosen_migrations):
            if not m.id in applied_migration_ids:
                continue

            click.echo(f"Revert migration {m.id}")
            for cmd in m.commands_of_down_script:
                click.echo(f"- {cmd.replace("\n", " ")} ", nl=False)
                cur.execute(cmd)
                click.echo("[ ok ]")
            cur.execute("delete from mitch.applied_migrations where id = %s", (m.id,))

    # TODO: Remove dependencies of chosen_migration, if no longer used by other migrations.


@cli.command()
@click.pass_context
def applied(ctx):
    load_applied_migrations()
    for migration in applied_migrations.values():
        if not migration.as_a_dependency:
            click.echo(f"{migration.id}")


@cli.command()
@click.option("except_ids", "--except", multiple=True)
@click.option("except_files", "--except-from-file", multiple=True, type=click.Path(exists=True, readable=True, file_okay=True, dir_okay=False, allow_dash=True))
@click.pass_context
def prune(ctx, except_ids: typing.List[str], except_files: typing.List[str]):
    repository = Repository(root_folder=Path.cwd())

    # Get to be installed ids
    to_be_installed_ids = set(except_ids)
    for f in except_files:
        lines = click.open_file(f, mode="r", encoding="utf-8").readlines()
        to_be_installed_ids.update(l.strip() for l in lines if not l.isspace())
    
    # If no ids were supplied, chose all explicitely installed migrations
    if len(to_be_installed_ids) == 0:
        to_be_installed_ids |= set(m.id for m in applied_migrations.values() if not m.as_a_dependency)

    # Get dangling migrations.
    to_be_installed_migrations = repository.by_ids(to_be_installed_ids)
    needed_migrations = list(repository.dependencies_of(*to_be_installed_migrations))
    dangling_migrations = [m.migration_on_disk for m in applied_migrations.values() if m.migration_on_disk not in needed_migrations and m.migration_on_disk]

    # Execute migrations in topological order
    with db.transaction(), db.cursor() as cur:
        for m in reversed(list(topologically_sorted_dependencies(*dangling_migrations))):
            if m not in dangling_migrations:
                continue

            click.echo(f"Revert migration {m.id}")
            for cmd in m.commands_of_down_script:
                click.echo(f"- {cmd.replace("\n", " ")} ", nl=False)
                cur.execute(cmd)
                click.echo("[ ok ]")
            cur.execute("delete from mitch.applied_migrations where id = %s", (m.id,))


cli.add_command(apply)
cli.add_command(unapply)
cli.add_command(applied)
cli.add_command(prune)


if __name__ == "__main__":
    cli()

# Roadmap:
# - [ ] Command to add migrations
# - [ ] Command to rework a migration, similar to sqitch rework.
# - [ ] Command to re-apply some migrations, similar to sqitch rebase.
# - [x] Command to apply migrations from a plan (like a pip install from a requirements file.)
# - [?] Command to unapply/apply migrations from a plan when changing git branches (like sqitch checkout)
# - [x] Command to remove all migrations that no-one depends on.
# - Add support for configurable target databases
#   - Take care of sensitive information. Credentials must be managable outside version control.
# - [x] Allow for relative paths in dependencies
# - [ ] Allow for multiple repositories (to fetch migrations from)
# - [ ] Allow for multiple targets (each with a separate toml file)
# - Test performance for hundreds/thousands of migrations
