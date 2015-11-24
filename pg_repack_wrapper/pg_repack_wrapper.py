#!/usr/bin/env python2

import argparse
import os
import subprocess
import psycopg2
import psycopg2.extras
import time
import logging

# TODO nice feature would be pg_stattuple support, for exact bloat calculations

SQL_TABLE_BLOAT = """
SELECT
  schemaname||'.'||tblname as table_name_full,
  pg_size_pretty(bloat_size::numeric) as bloat_size,
  round(bloat_ratio::numeric, 1) as bloat_ratio,
  pg_size_pretty(real_size::numeric) as table_size
FROM (
  /* START https://github.com/ioguix/pgsql-bloat-estimation/blob/master/table/table_bloat.sql */
SELECT current_database(), schemaname, tblname, bs*tblpages AS real_size,
  (tblpages-est_tblpages)*bs AS extra_size,
  CASE WHEN tblpages - est_tblpages > 0
    THEN 100 * (tblpages - est_tblpages)/tblpages::float
    ELSE 0
  END AS extra_ratio, fillfactor, (tblpages-est_tblpages_ff)*bs AS bloat_size,
  CASE WHEN tblpages - est_tblpages_ff > 0
    THEN 100 * (tblpages - est_tblpages_ff)/tblpages::float
    ELSE 0
  END AS bloat_ratio, is_na
  -- , (pst).free_percent + (pst).dead_tuple_percent AS real_frag
FROM (
  SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages,
    ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff,
    tblpages, fillfactor, bs, tblid, schemaname, tblname, heappages, toastpages, is_na
    -- , stattuple.pgstattuple(tblid) AS pst
  FROM (
    SELECT
      ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
        - CASE WHEN tpl_hdr_size%%ma = 0 THEN ma ELSE tpl_hdr_size%%ma END
        - CASE WHEN ceil(tpl_data_size)::int%%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%%ma END
      ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages,
      toastpages, reltuples, toasttuples, bs, page_hdr, tblid, schemaname, tblname, fillfactor, is_na
    FROM (
      SELECT
        tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,
        tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
        coalesce(toast.reltuples, 0) AS toasttuples,
        coalesce(substring(
          array_to_string(tbl.reloptions, ' ')
          FROM '%%fillfactor=#"__#"%%' FOR '#')::smallint, 100) AS fillfactor,
        current_setting('block_size')::numeric AS bs,
        CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
        24 AS page_hdr,
        23 + CASE WHEN MAX(coalesce(null_frac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END
          + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,
        sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS tpl_data_size,
        bool_or(att.atttypid = 'pg_catalog.name'::regtype) AS is_na
      FROM pg_attribute AS att
        JOIN pg_class AS tbl ON att.attrelid = tbl.oid
        JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
        JOIN pg_stats AS s ON s.schemaname=ns.nspname
          AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
        LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
      WHERE att.attnum > 0 AND NOT att.attisdropped
        AND tbl.relkind = 'r'
      GROUP BY 1,2,3,4,5,6,7,8,9,10, tbl.relhasoids
      ORDER BY 2,3
    ) AS s
  ) AS s2
) AS s3
/* END https://github.com/ioguix/pgsql-bloat-estimation/blob/master/table/table_bloat.sql */
) a
JOIN
  pg_class c ON c.relname = a.tblname
JOIN
  pg_namespace n ON n.oid = c.relnamespace AND n.nspname = a.schemaname
WHERE
  (%(min_bloat_ratio)s IS NULL OR bloat_ratio >= %(min_bloat_ratio)s)
  AND (%(min_bloat_size_mb)s::numeric IS NULL OR bloat_size/ 10^6 >= %(min_bloat_size_mb)s::numeric)
  -- pg_repack requires that tbls have a PK or a unique index
  AND EXISTS (select 1 from pg_index where indrelid = c.oid and indisvalid and (indisunique or indisunique))
ORDER BY
  bloat_ratio DESC
    """


def shell_exec_with_output(commands):
    process = subprocess.Popen(commands, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    exitcode = process.wait()
    output = process.stdout.read().strip()
    if exitcode != 0:
        logging.error('error executing: %s', commands)
        logging.error(output)
    return exitcode, output


def get_bloated_tables(min_bloat_ratio=None, min_bloat_size_mb=None):
    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.dbname, user=args.username)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(SQL_TABLE_BLOAT, {'min_bloat_ratio': min_bloat_ratio, 'min_bloat_size_mb': min_bloat_size_mb})
    return cur.fetchall()


def call_pg_repack(table_name_full, args, unknown_args):
    PG_REPACK_CMD = 'pg_repack'

    PG_REPACK_CMD += ' --table ' + table_name_full
    for k, v in vars(args).iteritems():
        if k in ['host', 'port', 'dbname', 'username']:
            PG_REPACK_CMD += ' --{} {}'.format(k, v)
    PG_REPACK_CMD += ' ' + ' '.join(unknown_args)
    logging.info('executing: %s', PG_REPACK_CMD)
    if not args.run:
        return

    retcode, output = shell_exec_with_output(PG_REPACK_CMD)
    if retcode != 0 and args.stop_on_error:
        logging.error('exiting because of a processing error')
        exit(1)
    logging.info(output)


args = None


def main():
    argp = argparse.ArgumentParser(description='A light wrapper around pg_repack command line tool, providing re-packing of only bloated tables, '
                                               'starting from the most bloated ones', add_help=False)

    argp.add_argument('--help', help='Show help', action='help')
    argp.add_argument('-h', '--host', help='PG host', required=True)
    argp.add_argument('-p', '--port', help='PG port', default=5432, type=int)
    argp.add_argument('-d', '--dbname', help='PG database', required=True)
    argp.add_argument('-U', '--username', help='PG user', default=os.getenv('USER'))    # password is assumed to be in .pgpass
    argp.add_argument('-T', '--wait-timeout', help='Max seconds to wait when acquiring an exclusive table lock. pg_repack default=60', required=True)

    argp.add_argument('--min-bloat-ratio', help='Min (relative) bloat ratio for table to be considered for re-packing', default=20, type=int)
    argp.add_argument('--min-bloat-size-mb', help='Min bloat size in MB for table to be considered for re-packing', default=10, type=int)
    argp.add_argument('-r', '--run', help='Do re-packing. Default is to just display to-be-affected tables', action='store_true')
    argp.add_argument('-t', '--table', help='Tables to possibly re-pack', action='append')
    argp.add_argument('-q', '--quiet', help='No chat, only errors (Cronjob mode)', action='store_true')
    argp.add_argument('--stop-on-error', help='Exit program on 1st re-packing error', action='store_true')

    global args
    args, unknown_args = argp.parse_known_args()

    logging.basicConfig(level=(logging.ERROR if args.quiet else logging.INFO), format='%(asctime)s (%(levelname)s) %(message)s')
    logging.info('args: %s, unknown_args: %s', args, unknown_args)

    retcode, out = shell_exec_with_output('which pg_repack')
    logging.info('Using pg_repack from path: %s', out)

    if args.run:
        logging.info('NOT in dry-run mode, sleeping 3s...')
        time.sleep(3)

    bloated = get_bloated_tables(min_bloat_ratio=args.min_bloat_ratio, min_bloat_size_mb=args.min_bloat_size_mb)

    if not len(bloated):
        logging.info('No matching tables found')
        return

    i = 0
    for b in bloated:
        if args.table and b['table_name_full'] not in args.table:
            continue
        logging.info('Doing table: "%s" (bloat ratio: %s, bloat_size: %s, table_size %s)', b['table_name_full'],
                                                                                          b['bloat_ratio'],
                                                                                          b['bloat_size'],
                                                                                          b['table_size'])
        call_pg_repack(b['table_name_full'], args, unknown_args)
        i += 1
    logging.info('')
    logging.info('Finished. %s tables processed.', i)


if __name__ == '__main__':
    main()
