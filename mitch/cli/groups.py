import click

@click.group()
def root():
    pass


@root.group()
def ls():
    pass


@root.group()
def add():
    pass