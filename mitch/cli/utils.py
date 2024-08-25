import psycopg

from ..repository import Repository
from ..utils import CompositeId


def complete_available_migration_id(ctx, param, incomplete):
    try:
        repository = Repository.from_closest_parent()
    except FileNotFoundError:
        return []
    else:
        return [
            str(m.id)
            for k, m in repository.migrations.items()
            if k[1].startswith(incomplete) or str(m.id).startswith(incomplete)
        ]


def complete_installed_migration_id(ctx, param, incomplete):
    with psycopg.connect() as db:
        cur = db.execute(
            """
            select repository_id, migration_id 
            from mitch.applied_migrations 
            where 
                migration_id like %(1)s 
                or (repository_id || ':' || migration_id) like %(1)s
            """,
            (incomplete + "%",),
        )
        return [str(CompositeId.from_tuple(row)) for row in cur.fetchall()]