#!/usr/bin/env python
# -*- coding: utf-8 -*-

from psycopg2.extras import DictCursor
from functools import partial
import psycopg2.extensions
import threading
import psycopg2
import datetime
import argparse
import logging
import termios
import select
import Queue
import yaml
import time
import math
import sys
import tty
import os
import re

CONFIG_FILE = os.path.expanduser('~/.niceupdate.conf')
POISON = object()
TIMEOUT = 0.2
DELTA = datetime.timedelta(seconds=60)
MINXLOGFREE = 5
IGNORE_SERVER_CHECK = False
EXPLAIN_ROWS_RE=re.compile("\(.*rows=(\d+)\s+width=\d+\)")

tty_store = '\033[s'
tty_restore = '\033[u'
tty_up = '\033[{0}A'
tty_erase = '\033[K'
tty_forward = '\033[{0}C'

global_write = threading.Lock()
# accept 'any', any number of spaces, and either an expression within () with
# any number of heading spaces, letters and trailing spaces, and s after the ),
# or a simple %s with any number of heading and trailing spaces. Case is ignored.
# Example of type 1 expression: any   (  %( name  )s )
# Example of type 2 expression: any ( %s    )

sql_param_pattern = re.compile('any\s*\(((\s*%\(\s*(\w+)\s*\))|\s*%)s\s*\)', re.I)


class PlaceholderParseException(Exception):

    pass


SQL_GET_TIMEOUT = "select current_setting('statement_timeout')"
SQL_SET_TIMEOUT = 'set statement_timeout = %s'
SQL_GETLOAD = "select val from public.nice_get_server_information('load1')"
SQL_GETXLOG = "select val/1024/1024/1024 as xlogfree from public.nice_get_server_information('xlogfreesize')"
SQL_DROP_PLPYTHONFUNC = 'drop function if exists public.nice_get_server_information(text)'
SQL_CREATE_PLPYTHONFUNC = \
    """
DO $INSTALL$
BEGIN

CREATE OR REPLACE FUNCTION public.nice_get_server_information(key IN text, val OUT real)
LANGUAGE plpythonu
AS
$BODY$
'''
This function provides two server diagnostic figures used by niceupdate.
For xlogfree it should not be called to often.
'''
import os

def get_load1_average():
    return os.getloadavg()[0]

def get_xlog_directory():
    "Get the xlog directory of the database cluster."
    if 'stmt_setting' in SD:
        plan = SD['stmt_setting']
    else:
        plan = plpy.prepare("SELECT name, setting FROM pg_catalog.pg_settings  where name = 'data_directory'")
        SD['stmt_setting']= plan
    rv = plpy.execute(plan)
# there should be an exception if this row is missing
    datadir = rv[0]['setting']
    return os.path.join(datadir, 'pg_xlog')

def get_mount_point(realpathname):
    "Get the mount point of the filesystem containing pathname"
    pathname = os.path.normcase(realpathname)
    parent_device = path_device = os.stat(pathname).st_dev
    while parent_device == path_device:
        mount_point = pathname
        pathname = os.path.dirname(pathname)
        if pathname == mount_point:
            break
        parent_device = os.stat(pathname).st_dev
    return mount_point

def get_fs_space(pathname):
    "Get the free space of the filesystem containing pathname in byte"
    stat = os.statvfs(pathname)
    free = stat.f_bavail * stat.f_bsize
    return free

def get_xlog_free_space():
    "Get size of available space on device for xlog directory"
    xlogdir = get_xlog_directory()
    realpath = os.path.realpath(xlogdir)
    mount_point = get_mount_point(realpath)
    return get_fs_space(mount_point)

if key == 'load1':
    return get_load1_average()
elif key == 'xlogfreesize':
    return get_xlog_free_space()

$BODY$
SECURITY DEFINER;
GRANT EXECUTE on FUNCTION public.nice_get_server_information(text) TO public;
END;
$INSTALL$;
   """


def getargs():
    argp = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                   description='executes a statement for the resultset of a query with load control of the server'
                                   ,
                                   epilog="""The config file can look like this at minimum:
database: customer
getid: select ad_id id from  zz_commons.appdomain
update: update zz_commons.appdomain set ad_id = ad_id where ad_id = %(id)s

One of the following should be given and greater than 0:
commitrows: 20
chunksize: 0

Following attribute are optional and would be overwritten if a command line arg is provided:
environment: integration
shard: 0
maxload: 2.0
vacuum_cycles: 10
vacuum_delay: 100
delay: 0
vacuum_table: schema.table
"""
)
    argp.add_argument('-f', '--file', dest='filename', required=True, help='yaml file for config data')
    argp.add_argument('-l', '--maxload', dest='maxload', type=float, metavar='FLOAT',
                      help='update unless load is higher than FLOAT')
    argp.add_argument('-C', '--commitrows', dest='commitrows', help='do commit every NUMBER of rows', metavar='NUMBER',
                      type=int)
    argp.add_argument('-t', '--test', dest='test', action='store_true', help='test parameter substitution and roll back'
                      , default=False)
    argp.add_argument('-d', '--delay', dest='delay', help='delay between executing consecutive statements', type=int)
    argp.add_argument('-E', '--environment', dest='environment', help='destination environment')
    argp.add_argument('-F', '--force', dest='force',
                      help='Do not perform neither xlog nor load check in cases of absence of server extension on target database'
                      , action='store_true')
    argp.add_argument('-S', '--shard', dest='shard', help='execute only on given shard')
    argp.add_argument('-v', '--verbose', dest='verbose', help='set log level DEBUG', action='store_true')
    argp.add_argument('-V', '--vacuumcycles', dest='vacuum_cycles', help='perform vacuum after NUMBER of commits',
                      metavar='NUMBER', type=int)
    argp.add_argument('-X', '--xlogminimum', dest='xlogminimun', type=int, help='minimum size of xlog partition in GB',
                      default=MINXLOGFREE)
    argp.add_argument('-Y', '--vacuumdelay', dest='vacuum_delay', help='vacuum_cost_delay (0...100) in ms', default=0,
                      type=int)
    argp.add_argument('-U', '--user', dest='user', help='user for database connect')
    argp.add_argument('-u', '--uninstall', dest='uninstall', action='store_true',
                      help='Uninstall server funtion from database specified in file')
    argp.add_argument('-i', '--install', dest='install', action='store_true',
                      help='Install server funtion on database specified in file')
    return argp.parse_args()


def load_config(args):

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    global MINXLOGFREE, IGNORE_SERVER_CHECK
    MINXLOGFREE = args.xlogminimun
    IGNORE_SERVER_CHECK = args.force
    with open(args.filename) as f:
        document = yaml.safe_load(f)
    logger.info('START: job file {} loaded.'.format(args.filename))
    document['test'] = args.test
    document['batchmode'] = not sys.stdout.isatty()
    document['shard'] = args.shard or document.get('shard', None)
    document['verbose'] = args.verbose
    document['commitrows'] = args.commitrows or document.get('commitrows', 0)
    document['maxload'] = args.maxload or document.get('maxload') or 2.0
    document['vacuum_cycles'] = args.vacuum_cycles or document.get('vacuum_cycles') or 0
    document['vacuum_delay'] = args.vacuum_delay or document.get('vacuum_delay') or 0
    document['environment'] = args.environment or document.get('environment', None)
    if not document['environment']:
        sys.stderr.write('Database environment missing.\n')
        sys.exit(1)
    if bool(document.get('commitrows')) == bool(document.get('chunksize')) or document.get('commitrows', 0) \
        + document.get('chunksize', 0) < 1:
        sys.stderr.write('Either commitrows or chunksize should be greater than 0.\n')
        sys.exit(1)
    if document['vacuum_cycles'] and not document['vacuum_table']:
        sys.stderr.write('No vacuum table specified\n')
        sys.exit(1)
    if document['vacuum_delay'] and (document['vacuum_delay'] > 100 or document['vacuum_delay'] < 0):
        sys.stderr.write('Specify value between 0...100 for vacuum delay\n')
        sys.exit(1)
    document['delay'] = args.delay or document.get('delay') or 0
    document['user'] = args.user or os.getenv('PGUSER') or os.getenv('USER')
    logger.debug(document)
    return document


def connect(phost, pport, pdbname, pusername, ppassword):
    """Returns tupel (connection, cursor)"""

    conn = psycopg2.connect(host=phost, database=pdbname, port=pport, user=pusername, password=ppassword,
                            application_name='niceupdate')
    cur = conn.cursor(cursor_factory=DictCursor)
    return conn, cur


def show(line, prefix, text):
    if line == 0:
        l_up = ''
    else:
        l_up = tty_up.format(line)
    mess = tty_store + l_up + '\r' + tty_erase + prefix + text + tty_restore
    global_write.acquire()
    sys.stdout.write(mess)
    sys.stdout.flush()
    global_write.release()


class Query(object):

    def __init__(self, conn, sql, name=None):
        if conn:
            if name:
                self.cur = conn.cursor(name=name, cursor_factory=DictCursor)
                self.cur.withhold = True
            else:
                self.cur = conn.cursor(cursor_factory=DictCursor)
        self.sql = sql
        self.test = False
        self.verbose = False
        self.queue = Queue.Queue()
        self.canceled = False
        self.queueIsEmpty = threading.Event()
        self.updChunkSize = 0

    def get_param_name(self, sql):
        '''Extract the name of placeholder from the any expression for chunked updates'''

        logger.debug(sql)
        names = set()
        anonymous = set()
        name = ''
        for m in sql_param_pattern.finditer(sql):
            name = m.group(3)  # name from %(%(name)s)
            if not name:  # # there is an anonymous placeholder
                name = m.group(0) + str(m.__hash__())  # # which should occur only once
                anonymous.add(name)
            else:
                names.add(name)
        name_count = len(names)
        anon_count = len(anonymous)
        logger.debug(names)
        logger.debug(anonymous)
        if name_count == 1 and anon_count == 0:
            return name
        elif name_count == 0 and anon_count == 1:
            return None
        else:
            raise PlaceholderParseException('Too many or few placeholders found')

    def argexec(self, *args, **kwargs):
        try:
            if args:
                self.cur.execute(self.sql, args)
            else:
                self.cur.execute(self.sql, kwargs)
        except psycopg2.extensions.QueryCanceledError, p:
            self.kindly.show_exception(p)
        except BaseException, e:
            self.kindly.cancel()
            self.kindly.show_exception(e)
            logger.debug(kwargs)
            logger.debug(args)
            logger.exception('query exception')

    def exec_query(self, param):
        """keyboard interruptable query execution, param should be an dict"""

        if self.test and self.verbose:
            self.kindly.show_message(self.cur.mogrify(self.sql, param))
        qth = threading.Thread(target=self.argexec, args=(), kwargs=param)
        qth.start()
        while True:
            qth.join(TIMEOUT)
            if not qth.isAlive():
                break
        return self.cur.rowcount > 0  # indicates that we were not canceled

    def process_queue(self):
        '''Execute give sql for each parameter from queue'''

        try:
            while not self.canceled:
                param = self.queue.get(True)
                if param == POISON or self.canceled:
                    break
                self.argexec(**param)
                if self.queue.empty():
                    self.queueIsEmpty.set()
                    self.queueIsEmpty.clear()
        except BaseException, e:
            self.kindly.cancel()
            self.kindly.show_exception(e)
        finally:
            if not self.queueIsEmpty.isSet():
                self.queueIsEmpty.set()

    def chunked_process_queue(self):
        '''Build a list of parameters and pass as array to server'''

        chunkCount = 0
        placeholder = self.get_param_name(self.sql)
        logger.debug('Parametername found: {}'.format(placeholder))
        chunk = []
        try:
            while not self.canceled:
                param = self.queue.get(True)
                if self.canceled:
                    break
                if not param == POISON:
                    chunk.append(param[0])
                    chunkCount += 1
                if chunk and (chunkCount >= self.updChunkSize or self.queue.empty() or param == POISON):
                    if not placeholder:
                        self.argexec(*(chunk, ))
                    else:
                        self.argexec(**{placeholder: chunk})
                    chunkCount = 0
                    del chunk[:]
                if param == POISON:
                    break
                if self.queue.empty():
                    self.queueIsEmpty.set()
                    self.queueIsEmpty.clear()
        except BaseException, e:
            logger.exception('Parameter: {}'.format(param))
            self.kindly.cancel()
            self.kindly.show_exception(e)
        finally:
            if not self.queueIsEmpty.isSet():
                self.queueIsEmpty.set()


class KindlyUpdate(object):

    def __init__(self, config, host, port, database):
        self.canceled = False
        self.commitrows = config['commitrows'] or config['chunksize']
        self.config = config
        self.conn, self.cur = connect(host, port, database, config['user'], None)
        self.dbname = database
        self.delay = config.get('delay', 0)
        self.eta = datetime.timedelta(seconds=0)
        self.fn_show = sys.stdout.write
        self.idcount = 0
        self.last_exception = ''
        self.max_load_average = self.config['maxload']
        self.maxcount = 0
        self.myquery = ''
        self.next_xlog_fetch_time = datetime.datetime.now()
        self.starttime = None
        self.statusmessage = ''
        self.test = self.config['test']
        self.thresholdlock = threading.Lock()
        self.vacuum_cycles = config.get('vacuum_cycles', 0)
        self.verbose = self.config.get('verbose', False)
        logger.debug('connected to {}'.format(database))

    def build_query(self, sql, name=None):
        q = Query(self.conn, sql, name)
        q.kindly = self
        return q

    def build_update_query(self):
        update_query = self.build_query(self.config.get('update'))
        update_query.test = self.test
        update_query.verbose = self.verbose
        update_query.updChunkSize = self.config.get('chunksize', 0)
        if update_query.updChunkSize:
            update_query.get_param_name(update_query.sql)  # there might be an exception, check before start thread
        return update_query

    def wait_and_rollback(self):
        self.myquery.queueIsEmpty.wait()
        self.cur.connection.rollback()
        self.show_message('changes rolled back')

    def on_commit(self, idcount):
        self.conn.commit()
        self.calc_eta()
        self.show_message('committed')
        if self.delay:
            time.sleep(self.delay)
        self.wait_for_load_stabilization()
        if self.vacuum_cycles and not self.canceled and idcount % (self.commitrows * self.vacuum_cycles) == 0:
            self.show_message('committed VACUUM')
            self.perform_vacuum()

    def build_update_thread(self):
        self.myquery = self.build_update_query()
        if self.myquery.updChunkSize > 0:
            updater = threading.Thread(target=self.myquery.chunked_process_queue, name=self.dbname)
        else:
            updater = threading.Thread(target=self.myquery.process_queue, name=self.dbname)
        return updater

    def run(self):
        try:
            updater = self.build_update_thread()
            updater.start()
            self.idcount = 0
            self.starttime = datetime.datetime.now()
            for row in self.getids():
                self.idcount += 1
                if self.canceled:
                    self.show_message('canceled')
                    break
                self.myquery.queue.put(row)
                if self.test:
                    self.wait_and_rollback()
                    break
                elif self.idcount % self.commitrows == 0:
                    self.myquery.queueIsEmpty.wait()
                    if self.canceled:
                        break
                    self.on_commit(self.idcount)
            self.myquery.queue.put(POISON)
            updater.join()
            if not (self.canceled or self.test):
                self.report_result()
        except PlaceholderParseException, e:
            self.cancel()
            self.last_exception = e
            logger.exception(e)
        except Exception, e:
            logger.exception(e)
            self.last_exception = e
            self.cancel()
            if not self.conn.closed:
                self.cur.connection.rollback()
        finally:
            if not self.conn.closed:
                self.conn.commit()
                self.cur.close()
                self.conn.close()

    def getids(self):
        '''Generator for rows from getid sql parameter'''

        self.maxcount = self.get_estimated_rowcount()
        # build a named cursor withhold
        q = self.build_query(self.config.get('getid'), 'getid')
        self.cur.itersize = max(self.cur.itersize, self.commitrows)
        self.show_message('')
        q.exec_query(None)
        self.conn.commit()
        self.wait_for_load_stabilization()
        for row in q.cur:
            yield row

    def get_estimated_rowcount(self):
        '''We estimate the row count of getid query using explain.
        Parse the first row in order to retrieve the number for rows.
        Return int'''

        explain = "explain " + self.config.get('getid')
        q = self.build_query(explain)
        s = None
        r = 0
        if q.exec_query(None):
            s = q.cur.fetchone()
        if s:
            m = EXPLAIN_ROWS_RE.search(s[0])
            if m:
                r = int(m.group(1))
        return r

    def get_eta(self):
        return self.eta

    def calc_eta(self):
        '''calculate estimate time to finish'''

        if self.starttime == None:
            return
        chunksRemain = self.maxcount - self.idcount
        avgDuration = ((datetime.datetime.now() - self.starttime) / self.idcount if self.idcount
                       > 0 else datetime.timedelta(seconds=0))
        eta = chunksRemain * avgDuration
        eta = datetime.timedelta(seconds=math.floor(eta.total_seconds()))
        self.eta = eta

    def get_message_column(self):
        """Messages start after progress information"""

        countlen = len(str(self.maxcount))
        namelen = len(self.dbname) + 2
        return 2 * countlen + namelen + 2

    def get_load_average(self):
        '''Retrieve server load1 as float'''

        self.cur.execute(SQL_GETLOAD)
        self.conn.commit()
        for row in self.cur:
            return row['val']

    def fetch_current_xlog(self):
        '''Retrieve xlog partition free space in GB'''

        self.cur.execute(SQL_GETXLOG)
        for row in self.cur:
            return row['xlogfree']
        self.conn.commit()

    def get_xlog_free(self):
        '''Retrieve xlog free size from server cluster not more often then DELTA seconds'''

        if self.next_xlog_fetch_time < datetime.datetime.now():
            self.next_xlog_fetch_time = datetime.datetime.now() + DELTA
            self.xlogfreesize = self.fetch_current_xlog()
        return self.xlogfreesize

    def check_precondition(self):
        if IGNORE_SERVER_CHECK:
            logger.warning('forced to ignore server check')
            return True
        try:
            a = self.get_load_average()
            if not type(a) is float:
                self.show_message('result type of load check does not match')
                return False
            if not type(self.fetch_current_xlog()) is float:
                logger.debug('xlog check failed')
                self.show_message('result type of xlog check does not match')
                return False
        except StandardError, e:
            self.show_message(str(e))
            logger.exception(e)
            return False
        return not ('e' in locals() and e)

    def wait_for_load_stabilization(self):
        '''Checks if load and xlog is below thresholds'''

        self.conn.autocommit = True
        while not self.canceled:
            if not self.is_load_ok():
                self.status_message = 'Wait for load {0}: '
                self.show_message('Wait for load {0}: currently {1}'.format(self.max_load_average, self.current_load))
                time.sleep(5)
                self.calc_eta()
            elif not self.is_xlog_ok():
                self.show_message('Waiting. xlog partition under {0}GB: {1}'.format(MINXLOGFREE, self.xlogfreesize))
                time.sleep(5)
                self.calc_eta()
            else:
                self.status_message = None
                break
        if not self.conn.closed:
            self.conn.autocommit = False

    def is_load_ok(self):
        if IGNORE_SERVER_CHECK:
            return True
        else:
            self.current_load = self.get_load_average()
            return self.current_load < self.max_load_average

    def is_xlog_ok(self):
        if IGNORE_SERVER_CHECK:
            return True
        else:
            return self.get_xlog_free() >= MINXLOGFREE

    def cancel(self):
        if not self.conn.closed:
            self.conn.cancel()
        self.canceled = True
        if self.myquery:
            self.myquery.canceled = True
            self.myquery.queue.put(POISON)

    def increment(self):
        self.thresholdlock.acquire()
        self.max_load_average += 1
        self.thresholdlock.release()
        self.show_message('increased threshold: {0}'.format(self.max_load_average))

    def decrement(self):
        self.thresholdlock.acquire()
        self.max_load_average -= 1
        self.thresholdlock.release()
        self.show_message('decreased threshold: {0}'.format(self.max_load_average))

    def perform_vacuum(self):
        v = self.build_query('vacuum analyze {0}'.format(self.config.get('vacuum_table')))
        self.conn.autocommit = True
        c = self.conn.cursor()
        c.execute(SQL_GET_TIMEOUT)
        current_timeout = c.fetchone()[0]
        c.execute(SQL_SET_TIMEOUT, ('0', ))
        if self.config.get('vacuum_delay'):
            delay = self.config.get('vacuum_delay')
            c.execute('set vacuum_cost_delay to %s', (delay, ))
        v.exec_query(None)
        c.execute(SQL_SET_TIMEOUT, (current_timeout, ))
        self.calc_eta()
        try:
            self.conn.autocommit = False
        except:
            pass
        self.wait_for_load_stabilization()

    def show_exception(self, exception):
        self.last_exception = exception
        self.show_message('')
        logger.exception(exception)

    def show_message(self, message):
        if self.config.get('batchmode'):
            return
        self.statusmessage = message
        pretty_exc = ' '.join(str(self.last_exception).splitlines())
        myline = '{current}/{maxcount} {error} {status}'.format(current=self.idcount, maxcount=self.maxcount,
                status=self.statusmessage, error=pretty_exc)
        self.fn_show(myline)

    def report_result(self):
        # since maxcount is an estimation we correct todocount after successful finish
        self.maxcount = self.idcount
        msg = '{dbname}: {donecount} of {todocount} done.'.format(dbname=self.dbname, donecount=self.idcount,
                todocount=self.maxcount)
        logger.info(msg)
        if self.config.get('batchmode'):
            sys.stdout.write(msg)
            sys.stdout.write('\n')
        else:
            self.show_message('Done')


class InputReader(object):

    def __init__(self):
        self.input_interrupt = None
        self.input_queue = Queue.Queue()

    def read_input(self):
        while not self.input_interrupt.acquire(False):
            readlist = select.select([sys.stdin], [], [], TIMEOUT)[0]
            if len(readlist) > 0:
                ch = readlist[0].read(1)
                self.input_queue.put(ch)


def install(pusername, sql, db):
    conn, cur = connect(db[1], db[2], db[3], pusername, None)
    cur.execute(sql)
    conn.commit()
    conn.close()


def server_install(config, args, dblist):
    '''Installs server extension'''

    if args.install:
        fn = partial(install, config['user'], SQL_CREATE_PLPYTHONFUNC)
    elif args.uninstall:
        fn = partial(install, config['user'], SQL_DROP_PLPYTHONFUNC)
    else:
        return
    map(fn, dblist)


def read_database_configuration():
    """First we try to load a python module implementing this reading of database list from somewhere.
    If it fails we try to load from ~/.niceupdate.conf.
    It returns a list of dicts."""

    res = None
    try:
        import custom_database_config
        res = custom_database_config.load_database_config()
    except ImportError:
        logger.info('no custom implementation for database config found')
    except BaseException, e:
        logger.exception('error while loading config from custom_database_config')

    import types
    import json
    if type(res) == types.ListType and len(res) > 0:
        return res
    # else: if this fails, we try user configuration
    try:
        with open(CONFIG_FILE, 'r') as f:
            res = json.load(f)
    except BaseException, e:
        logger.exception('Error reading ' + CONFIG_FILE)
        raise Exception(e)
    if type(res) == types.ListType and len(res) > 0:
        return res
    else:
        raise Exception('No database configuration found')


def get_dblist(config):
    '''Accept dictionary of configuration data.
    Returns a list of tuples (shardname, host, port, databasename)'''

    dblist = read_database_configuration()
    environment = config.get('environment')
    dbtype = config.get('database')
    # for environment and database type we expect one dictionary of shards per configuration
    dbconfig = reduce(lambda x, y: (y['shards'] if y['environment'] == environment and y['name'] == dbtype else x),
                      dblist, {})
    r = []
    shard = config.get('shard', None)
    logger.debug(dbconfig)
    for k, v in dbconfig.iteritems():
        if shard and k[-1] != str(shard):
            continue
        hostport, dbname = v.split('/')
        host, port = get_host_port(hostport)
        r.append((k, host, port, dbname))
    if not r:
        msg = "No database '{db}' found for environment '{env}' shard '{shard}'.".format(db=dbtype, env=environment,
                shard=shard)
        logger.warning(msg)
        raise Exception(msg)
    return r


def makespace(count):
    space = count * '\n'
    sys.stdout.write(space)


def get_host_port(hostport):
    host, _, port = hostport.partition(':')
    return host, int(port or 5432)


def start_threads(dbs, config):
    """returns a list of tuples (thread, workerobject)"""

    threadlist = []
    for line, (_, host, port, dbname) in enumerate(dbs, start=1):
        kindly = KindlyUpdate(config, host, port, dbname)
        kindly.fn_show = partial(show, line, '{0}: '.format(kindly.dbname))
        worker = threading.Thread(target=kindly.run, name=kindly.dbname)
        if not kindly.check_precondition():
            return []
        threadlist.append((worker, kindly))
    map(lambda x: x[0].start(), threadlist)
    return threadlist


def setup_input_thread():
    input = InputReader()
    input.input_interrupt = threading.Lock()
    input.input_interrupt.acquire()
    t = threading.Thread(target=input.read_input, name='input-thread')
    t.start()
    return input


def show_status_line(config, p_eta=0):
    if config.get('batchmode'):
        return
    vacuumon = ('ON' if config.get('vacuum_cycles') else 'OFF')
    message = \
        'i: increment d:decrement Threshold   MaxLoad: {threshold} Vacuum: {is_vacuum} VacuumDelay: {vacuum} ETA: {eta}'.format(threshold=config.get('maxload'
            ), vacuum=config.get('vacuum_delay'), is_vacuum=vacuumon, eta=p_eta or '')
    show(0, '', message)


def notify_input(input, threadlist, config):
    method = None
    if input == 'i':
        method = KindlyUpdate.increment
        config['maxload'] += 1
    elif input == 'd':
        method = KindlyUpdate.decrement
        config['maxload'] -= 1
    if method == None:
        return
    show_status_line(config, 2.3)
    for _, u in threadlist:
        method(u)


def setup_logger():
    global logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(filename='niceupdate.log')
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.propagate = False


def check_threads_and_statusupdate(threadlist, config):
    eta = datetime.timedelta(seconds=0)
    result = ''
    for t, k in threadlist:
        t.join(TIMEOUT)
        eta = (k.get_eta() if k.get_eta() > eta else eta)
        if not t.isAlive():
            if k.last_exception:
                result += str(k.last_exception) + '\n'
            threadlist.remove((t, k))
    show_status_line(config, eta)
    return result


def cancel_and_close(threadlist):
    for t, k in threadlist:
        k.cancel()
        k.conn.rollback()
        k.conn.close()


def process_dbs(dbs, config):
    '''main procedure processing configuration. Returns errormessage if any'''

    result = ''
    batchmode = config.get('batchmode')
    if not batchmode:
        makespace(len(dbs))
    threadlist = start_threads(dbs, config)
    if not threadlist:
        sys.exit('precondition not fulfilled')
    if not batchmode:
        old_settings = termios.tcgetattr(sys.stdin)
    try:
        if not batchmode:
            tty.setcbreak(sys.stdin.fileno())
            input = setup_input_thread()
        while len(threadlist) > 0:
            try:
                if not batchmode:
                    notify_input(input.input_queue.get(timeout=TIMEOUT), threadlist, config)
            except Queue.Empty:
                pass
            finally:
                r = check_threads_and_statusupdate(threadlist, config)
                if r:
                    result += r
    except KeyboardInterrupt:
        result += 'Interrupted '
        cancel_and_close(threadlist)
    finally:
        if not batchmode:
            input.input_interrupt.release()  # cancel input thread
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        while len(threadlist) > 0:
            result += check_threads_and_statusupdate(threadlist, config)
        if not batchmode:
            sys.stdout.write('\n')
    if result:
        return result


def main():
    setup_logger()
    args = getargs()
    config = load_config(args)
    try:
        dbs = get_dblist(config)
    except BaseException, e:
        sys.exit(str(e))
    if args.install or args.uninstall:
        server_install(config, args, dbs)
        sys.exit(0)
    result = process_dbs(dbs, config)
    sys.exit(result)


if __name__ == '__main__':
    main()
