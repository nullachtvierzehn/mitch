import click
import psycopg

from ..groups import ls
from ...repository import Repository
from ...target import PostgreSqlTarget

@ls.command("up")
@click.option("--include-dependencies/--without-dependencies", "-d/-D", default=False)
def list_up_migrations(include_dependencies: bool):
    target = PostgreSqlTarget(psycopg.connect())
    try:
        repository = Repository.from_closest_parent()
    except (FileNotFoundError, NotADirectoryError) as e:
        raise click.UsageError(str(e)) from e
    for migration in target.installed_migrations(
        repository, include_dependencies=include_dependencies
    ):
        click.echo(f"{migration.id}")
