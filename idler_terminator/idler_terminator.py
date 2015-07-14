#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import os
import yaml
import argparse
import logging

argp = argparse.ArgumentParser(description='Kill idle Postgres connections based on configured settings (per host, db, username, timeout). Meant to be run from Cron')
argp.add_argument('-U', '--user', dest='user')
group1 = argp.add_mutually_exclusive_group(required=True)
group1.add_argument('-r', '--run', dest='run', action='store_true', default=False)
group1.add_argument('-n', '--dry-run', dest='dry_run', action='store_true', default=False)
argp.add_argument('-c', '--config', dest='config', default='config.yml')
argp.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False)  # default is cronjob mode - no talk
args = argp.parse_args()

if not args.user:
    args.user = os.getenv('PGUSER')
    if not args.user:
        print '--user is required if no PGUSER set! user needs access (.pgpass) to clusters defined in --config file and should be superuser or owner of to-be-killed processes'
        exit(1)

logging.basicConfig(format='%(asctime)s %(message)s', level=(logging.DEBUG if args.verbose else logging.WARNING))


def kill_idle_connections(host, port, databases, roles_to_kill, max_idle_time, dry_run=False):

    def get_pg_version(cur):
        q = """select setting from pg_settings where name = 'server_version_num'"""
        cur.execute(q)
        val = cur.fetchone()['setting']  # will be in form of 90301
        ver = val[0] + val[2]
        return int(ver)

    conn = psycopg2.connect(database='postgres', host=host, port=port, user=args.user)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    sql = \
        """
        select
            pid,
            case when not %s then pg_terminate_backend(pid) else null end as pg_terminate_backend,
            usename,
            datname,
            query,
            (now()-query_start)::text as idle_time
        from pg_stat_activity
        where state = 'idle in transaction'
        and datname like any(%s)
        and usename like any(%s)
        and now()-query_start >= %s::interval
        and pid != pg_backend_pid()
    """
    if get_pg_version(cur) < 92:
        sql = \
            """
            select
                procpid as pid,
                case when not %s then pg_terminate_backend(procpid) else null end as pg_terminate_backend,
                usename,
                datname,
                current_query as query,
                (now()-query_start)::text as idle_time
            from pg_stat_activity
            where current_query = '<IDLE> in transaction'
            and datname like any(%s)
            and usename like any(%s)
            and now()-query_start >= %s::interval
            and procpid != pg_backend_pid()
        """
    cur.execute(sql, (dry_run, databases, roles_to_kill, max_idle_time))
    return cur.fetchall()


def take_latest(*vars):
    for v in reversed(vars):
        if v:
            return v
    raise Exception('no valid input found')


config = yaml.load(open(args.config))
logging.info('Read config %s', config)

for env in config['envs']:
    logging.info('Processing env: %s', env['name'])
    for name, params in env['hosts'].iteritems():
        logging.info('Processing - Name: %s, Params:%s', name, params)

        databases = take_latest(config['databases'], (env['databases'] if 'databases' in env else None),
                                (params['databases'] if 'databases' in params else None))
        roles_to_kill = take_latest(config['roles_to_kill'], (env['roles_to_kill'] if 'roles_to_kill'
                                    in env else None), (params['roles_to_kill'] if 'roles_to_kill' in params else None))
        max_idle_time = take_latest(config['max_idle_time'], (env['max_idle_time'] if 'max_idle_time'
                                    in env else None), (params['max_idle_time'] if 'max_idle_time' in params else None))

        killed = None
        try:
            killed = kill_idle_connections(params['host'], params['port'], databases, roles_to_kill, max_idle_time,
                                           args.dry_run)
        except Exception, e:
            logging.error('Caught an exception')
            logging.error('%s', e)
            continue
        if killed:
            for k in killed:
                if args.dry_run:
                    logging.warning('Would kill a query on %s:%s', params['host'], params['port'])
                else:
                    logging.warning('Killed a query on %s:%s', params['host'], params['port'])
                logging.warning('PID: %s, USER: %s, DB: %s, IDLE_TIME: %s, QUERY: "%s"', k['pid'], k['usename'],
                                k['datname'], k['idle_time'], k['query'])
        else:
            logging.info('No idlers found')

