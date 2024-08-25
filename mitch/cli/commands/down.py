import click
import typing
import psycopg
import sys

from ..groups import root as cli
from ..utils import complete_installed_migration_id
from ...repository import Repository
from ...target import PostgreSqlTarget


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
    confirm_migrations = list(
        m
        for m, a in t.with_applications(dependants)
        if a and m not in chosen_migrations
    )
    confirm_migrations.sort(key=lambda m: m.id)
    if not yes and len(confirm_migrations) > 0:
        click.echo(f"The following migrations must be removed, too:")
        for m in confirm_migrations:
            click.echo(f"- {m.id}")
        if not click.confirm("Do you want to remove them?"):
            sys.exit(0)

    # Execute migrations in topological order
    with t.transaction():
        t.down(*(m for m, a in t.with_applications(dependants) if a))

        # Prune migrations, if requested.
        if prune:
            click.echo("Prune stale dependencies...")
            t.prune(repository)
