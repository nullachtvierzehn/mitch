import click
from pathlib import Path
from typing import Optional
import typing
from datetime import datetime, UTC

from ..groups import add
from ..utils import complete_available_migration_id
from ...repository import Repository


@add.command("migration")
@click.argument(
    "path",
    type=click.Path(
        exists=False,
        writable=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        path_type=Path,
    ),
)
@click.option("--id", type=str, default=None)
@click.option("--transactional/--non-transactional", "-t/-T", default=True)
@click.option("--idempotent/--non-idempotent", "-i/-I", default=True)
@click.option(
    "--dependencies",
    "-d",
    type=str,
    multiple=True,
    default=[],
    shell_complete=complete_available_migration_id,
)
def add_migration(
    path: Path,
    id: Optional[str],
    transactional: bool,
    idempotent: bool,
    dependencies: typing.List[str],
):
    if path.exists():
        raise click.UsageError(
            f"Cannot create migration, because {path} already exists."
        )
    try:
        repository = Repository.from_closest_parent()
    except (FileNotFoundError, NotADirectoryError) as e:
        raise click.UsageError(str(e)) from e

    # Create Id
    if not id:
        id = str(path.relative_to(repository.root_folder))

    # Check dependencies, if given.
    resolved_dependencies = []
    for d in dependencies:
        try:
            resolved_dependencies.append(repository.by_id(d))
        except KeyError as e:
            raise click.UsageError(f"Migration {d} does not exist.") from e

    # Create folder and files
    path.mkdir(parents=True, exist_ok=False)
    path.joinpath("up.sql").open("w", encoding="utf-8").write(f"-- deploy {id}\n")
    path.joinpath("down.sql").open("w", encoding="utf-8").write(f"-- revert {id}\n")
    with path.joinpath("migration.toml").open("w", encoding="utf-8") as fp:
        fp.write(f"id = {repr(id)}\n")
        fp.write(f'author = ""\n')
        fp.write(f"created_at = {repr(datetime.now(UTC).isoformat())}\n")
        fp.write("transactional = {0}\n".format("true" if transactional else "false"))
        fp.write("idempotent = {0}\n".format("true" if idempotent else "false"))
        if not resolved_dependencies:
            fp.write("dependencies = []\n")
        else:
            fp.write("dependencies = [\n")
            for d in resolved_dependencies:
                fp.write(f"    {repr(str(d.id))},\n")
            fp.write("]")
