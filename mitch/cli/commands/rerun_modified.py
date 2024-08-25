import click
import typing
import psycopg
import sys

from ..groups import root as cli
from ..utils import complete_installed_migration_id
from ...repository import Repository
from ...target import PostgreSqlTarget


@cli.command()
@click.option("--yes", is_flag=True, default=False)
@click.argument("migration", nargs=-1, shell_complete=complete_installed_migration_id)
def rerun_modified(migration: typing.List[str], yes: bool):
    target = PostgreSqlTarget(psycopg.connect())
    try:
        repository = Repository.from_closest_parent()
    except (FileNotFoundError, NotADirectoryError) as e:
        raise click.UsageError(str(e)) from e

    # Get modified migrations.
    modified = set(target.modified_migrations(repository))
    selected = set(repository.by_ids(migration))
    if len(selected) > 0:
        # Optionally restrict the operation to the given ids.
        modified &= selected
    if len(modified) == 0:
        return sys.exit(0)

    # Tell about selected, but unmodified migrations.
    if unmodified := selected - modified:
        click.echo(
            f"The following migrations have not been modified and don't need to be re-runned:"
        )
        for m in unmodified:
            click.echo(f"- {m.id}")

    # Fetch dependants, because they must be reverted first.
    with_dependants = list(
        (m, a)
        for (m, a) in target.with_applications(repository.dependants_of(modified))
        if a
    )

    # Confirm migrations that must be taken down but weren't explicitely selected.
    to_be_confirmed = list(m for m, a in with_dependants if m not in selected)
    if to_be_confirmed and not yes:
        click.echo(f"Must also re-run the following migrations:")
        for m in to_be_confirmed:
            click.echo(f"- {m.id}")
        if not click.confirm("Do you want to re-run them?"):
            return sys.exit(0)

    with target.transaction():
        target.down(*[m for (m, a) in with_dependants])
        for m, a in reversed(with_dependants):
            target.up(m, as_dependency=a.is_dependency)