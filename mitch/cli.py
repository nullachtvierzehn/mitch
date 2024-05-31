#!/usr/bin/env python

import typing
import sys

import click
import psycopg

from .repository import Repository
from .target import PostgreSqlTarget
from .utils import CompositeId

def complete_available_migration_id(ctx, param, incomplete):
    try:
        repository = Repository.from_closest_parent()
    except FileNotFoundError:
        return []
    else:
        return [
            str(m.id) for k, m in repository.migrations.items() 
            if k[1].startswith(incomplete) or str(m.id).startswith(incomplete)
        ]


def complete_installed_migration_id(ctx, param, incomplete):
    with psycopg.connect() as db:
        cur = db.execute(
            """
            select repository_id, migration_id 
            from mitch.applied_migrations 
            where 
                migration_id like %(1)s 
                or (repository_id || ':' || migration_id) like %(1)s
            """, 
            (incomplete + "%",)
        )
        return [str(CompositeId.from_tuple(row)) for row in cur.fetchall()]


@click.group()
def cli():
    pass


@cli.command("up")
@click.option("--target", "-t", default="default")
@click.option("files", "--from-file", multiple=True, type=click.Path(exists=True, readable=True, file_okay=True, dir_okay=False, allow_dash=True))
@click.option("--save", type=click.Path(writable=True, file_okay=True, dir_okay=False, allow_dash=False), default=None)
@click.option("--as-dependency", is_flag=True, default=False)
@click.argument("migration", nargs=-1, shell_complete=complete_available_migration_id)
def up_migration(migration: typing.List[str], files: typing.List[str], target: str, as_dependency: bool, save: str | None):
    try:
        repository = Repository.from_closest_parent()
    except (FileNotFoundError, NotADirectoryError) as e:
        raise click.UsageError(str(e)) from e
    t = PostgreSqlTarget(psycopg.connect())

    # Choose migrations by ids.
    chosen_migration_ids = set(migration)
    for f in files:
        lines = click.open_file(f, mode="r", encoding="utf-8").readlines()
        chosen_migration_ids.update(l.strip() for l in lines if not l.isspace())
    chosen_migrations = list(repository.by_ids(chosen_migration_ids))

    # Execute migrations in topological order
    with t.transaction():
        for m, a in t.with_applications(repository.dependencies_of(chosen_migrations)):
            # Is explicitely chosen.
            is_explicit = m in chosen_migrations

            # Was explicitely chosen before
            if a:  
                is_explicit |= not a.is_dependency

            # Should explicitely be marked as a dependency.
            if as_dependency and m in chosen_migrations:
                is_explicit = False
            
            is_dependency = not is_explicit

            # Run migration, if not already applied.
            if not a:
                t.up(m, as_dependency=is_dependency)
            
            # Skip migration, if already applied with the same script.
            elif a.matches(m):
                click.echo(f"Migration {m.id} already applied. [ skipped ]")
                #click.echo(f"Shoud be marked as dependency: {is_dependency}")
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
@click.option("--target", "-t", default="default")
@click.option("--yes", is_flag=True, default=False)
@click.option("--prune", is_flag=True, default=False)
@click.argument("migration", nargs=-1, shell_complete=complete_installed_migration_id)
def down(migration: typing.List[str], yes: bool, prune: bool, target: str):
    # Fetch migration(s)
    try:
        repository = Repository.from_closest_parent()
    except (FileNotFoundError, NotADirectoryError) as e:
        raise click.UsageError(str(e)) from e
    t = PostgreSqlTarget(psycopg.connect())
    
    chosen_migrations = set(repository.by_ids(migration))
    dependants = list(repository.dependants_of(chosen_migrations))

    # Confirm migrations that must be taken down but weren't explicitely selected.
    confirm_migrations = list(m for m, a in t.with_applications(dependants) if a and m not in chosen_migrations)
    confirm_migrations.sort(key=lambda m: m.id)
    if not yes and len(confirm_migrations) > 0:
        click.echo(f"The following migrations will be removed:")
        for m in confirm_migrations:
            click.echo(f"- {m.id}")
        if not click.confirm("Do you want to remove them?"):
            sys.exit(0)

    # Execute migrations in topological order
    with t.transaction():
        for m, a in t.with_applications(dependants):
            if a:
                t.down(m)
    
        # Prune migrations, if requested.
        if prune:
            click.echo("Prune stale dependencies...")
            t.prune(repository)


@cli.command()
@click.option("except_ids", "--except", multiple=True,  shell_complete=complete_installed_migration_id)
@click.option("except_files", "--except-from-file", multiple=True, type=click.Path(exists=True, readable=True, file_okay=True, dir_okay=False, allow_dash=True))
def prune(except_ids: typing.List[str], except_files: typing.List[str]):
    try:
        repository = Repository.from_closest_parent()
    except (FileNotFoundError, NotADirectoryError) as e:
        raise click.UsageError(str(e)) from e
    t = PostgreSqlTarget(psycopg.connect())

    # Get migrations that should remain installed.
    to_be_installed_ids = set(except_ids)
    for f in except_files:
        to_be_installed_ids.update(
            line.strip() 
            for lines in click.open_file(f, mode="r", encoding="utf-8").readlines()
            for line in lines if not line.isspace()
        )
    to_be_installed = list(repository.by_ids(to_be_installed_ids))

    # Remove all migrations, except the ones that are to be installed.
    t.prune(repository, except_migrations=to_be_installed)


@cli.group()
def ls():
    pass

@ls.command("up")
@click.option("--include-dependencies/--without-dependencies", "-d/-D", default=False)
def list_migrations_if_up(include_dependencies: bool):
    target = PostgreSqlTarget(psycopg.connect())
    try:
        repository = Repository.from_closest_parent()
    except (FileNotFoundError, NotADirectoryError) as e:
        raise click.UsageError(str(e)) from e
    for migration in target.installed_migrations(repository, include_dependencies=include_dependencies):
        click.echo(f"{migration.id}")


@ls.command()
def available():
    target = PostgreSqlTarget(psycopg.connect())
    try:
        repository = Repository.from_closest_parent()
    except (FileNotFoundError, NotADirectoryError) as e:
        raise click.UsageError(str(e)) from e
    
    for m, a in target.with_applications(repository.migrations.values()):
        if not a:
            click.echo(f"{m.id}")
        elif a.is_dependency:
            click.echo(f"{m.id} (applied as dependency)")
        else:
            click.echo(f"{m.id} (applied)")


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
