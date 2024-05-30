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

from .repository import Repository
from .target import PostgreSqlTarget


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.option("--target", "-t", default="default")
@click.option("files", "--from-file", multiple=True, type=click.Path(exists=True, readable=True, file_okay=True, dir_okay=False, allow_dash=True))
@click.option("--save", type=click.Path(writable=True, file_okay=True, dir_okay=False, allow_dash=False), default=None)
@click.option("--as-dependency", is_flag=True, default=False)
@click.argument("migration", nargs=-1)
@click.pass_context
def apply(ctx, migration: typing.List[str], files: typing.List[str], target: str, as_dependency: bool, save: str | None):
    repository = Repository(root_folder=Path.cwd())
    t = PostgreSqlTarget(psycopg.connect())

    # Choose migrations by ids.
    chosen_migration_ids = set(migration)
    for f in files:
        lines = click.open_file(f, mode="r", encoding="utf-8").readlines()
        chosen_migration_ids.update(l.strip() for l in lines if not l.isspace())
    chosen_migrations = list(repository.by_ids(chosen_migration_ids))

    # Execute migrations in topological order
    with t.transaction():
        for m, a in t.with_applications(repository.dependencies_of(*chosen_migrations)):
            # Migrations shoud be installed as a dependency, if they are not explicitely chosen.
            is_dependency = m not in chosen_migrations

            # Do not update the dependency flag, if the migration has been already applied.
            if a:
                is_dependency &= a.is_dependency

            # Furthermore, they should be installed as a dependency, if explicitely flagged as a dependency.
            is_dependency |= as_dependency and m in chosen_migrations
            
            # Run migration, if not already applied.
            if not a:
                t.up(m, as_dependency=is_dependency)
            
            # Skip migration, if already applied with the same script.
            elif a.matches(m):
                click.echo(f"Migration {m.id} already applied. [ skipped ]")
                t.fix_hashes_and_status(m, is_dependency=is_dependency)
            
            # Re-run idempotent migrations, if already applied with a different script.
            elif m.idempotent:
                if click.confirm(f"Migration {m.id} has been applied with a different script, but is marked as idempotent. Try to reapply?"):
                    t.up(m, as_dependency=is_dependency)
                else: 
                    raise ValueError(f"Migration {m.id} has been applied with a different script")
            
            # Fail for non-idempotent migrations, if already applied with a different script.
            else:
                raise ValueError(f"Migration {m.id} has been applied with a different script")
            
            if save and not is_dependency:
                # FIXME: Check, if migration has already been added to the file.
                click.open_file(save, mode="a", encoding="utf-8").write(f"{m.id}\n")


@cli.command()
@click.option("--with-dependencies/--without-dependencies", "-d/-D", default=False)
@click.option("--target", "-t", default="default")
@click.argument("migration", nargs=-1)
@click.pass_context
def unapply(ctx, migration: typing.List[str], with_dependencies: bool, target: str):
    # Fetch migration(s)
    repository = Repository(root_folder=Path.cwd())
    t = PostgreSqlTarget(psycopg.connect())
    chosen_migrations = repository.by_ids(migration)

    # Execute migrations in topological order
    with t.transaction():
        for m, a in t.with_applications(repository.dependants_of(*chosen_migrations)):
            if not a:
                continue
            else:
                t.down(m)


@cli.command()
@click.pass_context
def applied(ctx):
    target = PostgreSqlTarget(psycopg.connect())
    for application in target.applications.values():
        if not application.is_dependency:
            click.echo(f"{application.migration_id}")


@cli.command()
@click.option("except_ids", "--except", multiple=True)
@click.option("except_files", "--except-from-file", multiple=True, type=click.Path(exists=True, readable=True, file_okay=True, dir_okay=False, allow_dash=True))
@click.pass_context
def prune(ctx, except_ids: typing.List[str], except_files: typing.List[str]):
    repository = Repository(root_folder=Path.cwd())
    t = PostgreSqlTarget(psycopg.connect())

    # Get to be installed ids
    to_be_installed_ids = set(except_ids)
    for f in except_files:
        lines = click.open_file(f, mode="r", encoding="utf-8").readlines()
        to_be_installed_ids.update(l.strip() for l in lines if not l.isspace())
    
    # If no ids were supplied, chose all explicitely installed migrations
    if len(to_be_installed_ids) == 0:
        to_be_installed_ids |= set(a.migration_id for a in t.applications.values() if not a.is_dependency)

    # Get dangling migrations.
    to_be_installed_migrations = repository.by_ids(to_be_installed_ids)
    needed_migrations = list(repository.dependencies_of(*to_be_installed_migrations))
    dangling_migrations = [m for a, m in repository.with_migrations(t.applications.values()) if m and m not in needed_migrations]

    # Execute migrations in topological order
    with t.transaction():
        for m in reversed(list(repository.dependencies_of(*dangling_migrations))):
            if m in dangling_migrations:
                t.down(m)


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
