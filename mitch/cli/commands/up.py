import click
import typing
import psycopg

from ..groups import root as cli
from ..utils import complete_available_migration_id
from ...repository import Repository
from ...target import PostgreSqlTarget


@cli.command("up")
@click.option("--target", "-t", default="default")
@click.option(
    "files",
    "--from-file",
    multiple=True,
    type=click.Path(
        exists=True, readable=True, file_okay=True, dir_okay=False, allow_dash=True
    ),
)
@click.option(
    "--save",
    type=click.Path(writable=True, file_okay=True, dir_okay=False, allow_dash=False),
    default=None,
)
@click.option("--as-dependency", is_flag=True, default=False)
@click.argument("migration", nargs=-1, shell_complete=complete_available_migration_id)
def up_migration(
    migration: typing.List[str],
    files: typing.List[str],
    target: str,
    as_dependency: bool,
    save: str | None,
):
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
        deploy = list(
            t.with_applications(repository.dependencies_of(chosen_migrations))
        )
        n = str(len(deploy))
        nn = len(n)
        for i, (m, a) in enumerate(deploy):
            click.echo(f"[ {i+1: >{nn}} / {n} ] Run migration {m.id}")

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
                # click.echo(f"Shoud be marked as dependency: {is_dependency}")
                t.fix_hashes_and_status(m, is_dependency=is_dependency)

            # Re-run idempotent migrations, if already applied with a different script.
            elif m.idempotent:
                if click.confirm(
                    f"Migration {m.id} has been applied with a different script, but is marked as idempotent. Try to reapply?"
                ):
                    t.up(m, as_dependency=is_dependency)
                else:
                    raise ValueError(
                        f"Migration {m.id} has been applied with a different script"
                    )

            # Fail for non-idempotent migrations, if already applied with a different script.
            else:
                raise ValueError(
                    f"Migration {m.id} has been applied with a different script"
                )

            if save and not is_dependency:
                # FIXME: Check, if migration has already been added to the file.
                click.open_file(save, mode="a", encoding="utf-8").write(f"{m.id}\n")
