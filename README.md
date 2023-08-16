# acid-tools

**Various tools around PostgreSQL monitoring/management (mostly Python scripts)**

All scripts have a --help flag to explain usage.

## idler_terminator

A script meant to be run in Cron, to kill connections that are "idle in transaction" for too long.
Rules for "idle time", host, database and username exclusion/inclusion are configurable from a yaml file.

## niceupdate

A script for performing bigger data updates in a safe, controlled and observable (progress indication) manner by chunking
the workload and by reading load/xlog feedback from the DB host. Databases, update commands and chunk sizes are configurable via a yaml file (see --help).
More information on tool's [Wiki page] (https://github.com/kmoppel/acid-tools/wiki/Niceupdate).

## pg_loggrep

A log scanner with various input filters and output modes, including simple aggregations and console based error graphs.
Assumes "csvlog" logging format for input log files.
User should be in the "pg_log" folder or specify the path to this folder via -g/--globpath flag. Live tailing (-t/--tail) possible.

*Usage samples:*

Show errors (ERROR+FATAL severity) from last 60 minutes caused by users with "robot" in their username

```
./pg_loggrep -e -m 60 -u robot
```

Show client connection statistics by database/user + general statistics for the current day (default time unit if --minutes/--days not specified)

```
./pg_loggrep --conns
```

Show 5 most frequent warning patterns (patterns are created by stripping string literals from the 'message' part of log entries) for the last hour

```
./pg_loggrep --top-patterns 5 -m 60 --severity WARNING
```

## pg_repack_wrapper

A light wrapper around [pg_repack](https://github.com/reorg/pg_repack) tool, providing re-packing of only bloated tables, starting from the most bloated ones.

## psql_switch

Generates wrappers for the "psql" command line tool, connecting directly to the right database by name and providing
handy status information scripts. Needs the libpq "connection service file" (~/.pg_service.conf) to be there for input.

*Usage samples:*

First we need to generate the "psql" links (symlinks will be created in the CWD, so one should add it to $PATH) for the DBs defined in the service file. Then we can use shortcuts as usual "psql" commands or use the builtin helpers by providing predefined flags.

```
./psql_switch --generate-links
```

To just connect to the DB defined as service "mydb1" in the connection service file

```
./psql_mydb1
```

Show basic cluster information: [current_database, version, pg_is_in_recovery, inet_server_addr, inet_server_port, pg_postmaster_start_time, pg_conf_load_time, data_directory]

```
./psql_mydb1 --i[nfo]
```

Show currently active (executing) backends: [datname, usename, state, waiting, pid, query, age]

```
./psql_mydb1 --a[ctive]
```

Kill backends with PIDs x and y

```
./psql_mydb1 --kill x y
```

Many more flags..., see --help for a listing

```
./psql_mydb1 --help
```
