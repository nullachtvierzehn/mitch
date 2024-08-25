import click
import psycopg

from ..groups import ls
from ...repository import Repository
from ...target import PostgreSqlTarget


@ls.command("modified")
def list_modified_migrations():
    target = PostgreSqlTarget(psycopg.connect())
    try:
        repository = Repository.from_closest_parent()
    except (FileNotFoundError, NotADirectoryError) as e:
        raise click.UsageError(str(e)) from e
    for migration in target.modified_migrations(repository):
        click.echo(f"{migration.id}")