import click
import typing
import psycopg
import sys

from ..groups import root as cli
from ..utils import complete_installed_migration_id
from ...repository import Repository
from ...target import PostgreSqlTarget


@cli.command()
@click.option(
    "except_ids",
    "--except",
    multiple=True,
    shell_complete=complete_installed_migration_id,
)
@click.option(
    "except_files",
    "--except-from-file",
    multiple=True,
    type=click.Path(
        exists=True, readable=True, file_okay=True, dir_okay=False, allow_dash=True
    ),
)
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
            for line in lines
            if not line.isspace()
        )
    to_be_installed = list(repository.by_ids(to_be_installed_ids))

    # Remove all migrations, except the ones that are to be installed.
    t.prune(repository, except_migrations=to_be_installed)