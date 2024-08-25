import click
import psycopg

from ..groups import ls
from ...repository import Repository
from ...target import PostgreSqlTarget

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