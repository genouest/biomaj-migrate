

# Migration

## Setup

Make sure Biomaj is installed and running

A "biomaj-cli.py --status" should work.

Make sure that data.dir is the same for old and new install.
New version is fully compatible with the old release directory management.

Running twice the migration script will not overwrite existing values.

## One same server

Use a different properties location, migration script will create new property files based on old ones.


## On different server

Copy old properties to the new server (/etc/biomaj/db_properties)

data.dir should be mounted on the new server.

If a new data.dir is expected, then do not run any migration and start from a scratch new biomaj install.

## Process scripts

Process script are compatible, and can be kept on previous location, simply do not forget to set its location in new configuration file.

## Running migration

Migration script will copy properties files to the new conf.dir, modifying old scripts on-the-fly to the new syntax.
It will import all existing productio directory info in the new database.
Production directories are not modified nor copied/moved.

    python bin/biomaj-migrate.py --config global.properties --oldconfig ../oldbiomaj/biomaj/db_properties/global.properties

OR specifying db parameters

    python bin/biomaj-migrate.py --config global.properties --oldconfig ../oldbiomaj/biomaj/db_properties/global.properties --user biomaj --password 'XXX' --host 'YYY' --database biomaj_log


## Checks

  "biomaj-cli --status" should show the old databases.

# Requirements

mysql-devel
