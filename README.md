# How do I use this garbage?

Good point, bad question. Essentially, theres a ./migrations folder that holds all your migrations similar to diesel (the only other migrations thing ive used so dont think I have any experience). ./migrations/vagabond holds an ordered list of all migrations, and a vagabond table in the database holds the currently applied migration.

When the binary is called without a subcommand, it will display a list of applied (green) and unapplied (red) migrations.

## Commands

- init - Initializes the directory
- new \<name\> - Makes a ./migrations subdirectory and appends to the vagabond
- redo - Redos the applied migration
- rollback - Rolls back the applied migration
- apply - Applies the next unapplied migration
- delete - Deletes all unapplied migrations. pretty dangerous

goodbye and have an excellent day