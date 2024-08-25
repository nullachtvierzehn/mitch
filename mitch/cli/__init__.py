#!/usr/bin/env python

from .groups import root
from . import commands

if __name__ == "__main__":
    root()

# Roadmap:
# - [x] Command to add migrations
# - [ ] Command to rework a migration, similar to sqitch rework.
# - [x] Command to re-apply some migrations, similar to sqitch rebase.
# - [x] Command to apply migrations from a plan (like a pip install from a requirements file.)
# - [?] Command to unapply/apply migrations from a plan when changing git branches (like sqitch checkout)
# - [x] Command to remove all migrations that no-one depends on.
# - Add support for configurable target databases
#   - Take care of sensitive information. Credentials must be managable outside version control.
# - [x] Allow for relative paths in dependencies
# - [x] Allow for multiple repositories (to fetch migrations from)
# - [ ] Allow for multiple targets (each with a separate toml file)
# - Test performance for hundreds/thousands of migrations
