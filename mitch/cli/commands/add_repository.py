import click
from pathlib import Path
from typing import Optional

from ..groups import add
from ...repository import Repository

@add.command("repository")
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
@click.option("--name", type=str, default=None)
def add_repository(path: Path, name: Optional[str]):
    if path.exists():
        raise click.UsageError(
            f"Cannot create repository, because {path} already exists."
        )

    if not name:
        try:
            repository = Repository.from_closest_parent()
        except (FileNotFoundError, NotADirectoryError) as e:
            name = str(path.relative_to(Path.cwd()))
        else:
            name = str(path.relative_to(repository.root_folder))

    # Create folder and files
    path.mkdir(parents=True, exist_ok=False)
    with path.joinpath("mitch.toml").open("w", encoding="utf-8") as fp:
        fp.write(f"[repository]\n")
        fp.write(f"name = {repr(name)}\n")
        fp.write(f'maintainer = ""\n')