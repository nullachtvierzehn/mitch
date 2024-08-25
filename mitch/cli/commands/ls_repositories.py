import click

from ..groups import ls
from ...repository import Repository

@ls.command()
def repositories():
    repository = Repository.from_closest_parent()
    for repo in [repository.root, *repository.root.subrepositories.values()]:
        click.echo(f"{repo.name}")