#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re
import argparse
import logging
import glob
import time
from datetime import datetime
from datetime import timedelta
import csv
import os
from collections import defaultdict
import gzip


DEFAULT_GLOB_PATH = '.'     # '/var/lib/postgresql/*/main/pg_log'
PG_LOG_NAMING = '/postgresql-{curdate}*.csv*'
DEFAULT_GLOB = (DEFAULT_GLOB_PATH + PG_LOG_NAMING).format(curdate=datetime.now().strftime('%Y-%m-%d'))
POSTGRES_DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%f %Z'

argp = \
    argparse.ArgumentParser(description='Scans PostgreSQL logs and displays log lines or statistics according to given filters. \
     Assumes default "csvlog" logging format and by default being in the pg_log folder or specifying a folder e.g. with --globpath=/var/lib/postgresql/*/main/pg_log', add_help=True)
group_summaries = argp.add_mutually_exclusive_group()
# Input
group1 = argp.add_mutually_exclusive_group()
group1.add_argument('-s', '--stdin', action='store_true', default=False, help='Read input from stdin')
group1.add_argument('-f', '--file', default=None, help='File to grep. Can be Gzip')
group1.add_argument('-g', '--globpath', default=DEFAULT_GLOB_PATH, help='Glob pattern for logfiles [default.: {}]'.format(DEFAULT_GLOB_PATH))
group_summaries.add_argument('-t', '--tail', action='store_true', default=False, help='Keep running and checking for new input/files')
# Filters
argp.add_argument('keyword', metavar='KEYWORD', type=str, nargs='?', help='Text to grep for')
argp.add_argument('-q', '--queries', action='store_true', default=False, help='Show only SQL statements')
argp.add_argument('-d', '--dbname', help='Show only entries from DBs matching given substring')     # TODO regex
argp.add_argument('-n', '--no-noise', action='store_true', default=False, help='Try to remove PgAdmin3 noise')
argp.add_argument('-p', '--people', action='store_true', default=False, help='Show only entries from human users')    # by default show both
argp.add_argument('-r', '--robots', action='store_true', default=False, help='Show only entries from robots (zomcat*, robot*)')    # TODO regex
argp.add_argument('-u', '--user', help='Show only entries matching given substring')     # TODO incl/excl usersm, regex
group_time_filters = argp.add_mutually_exclusive_group()
group_time_filters.add_argument('-m', '--minutes', help='Show only entries younger than given minutes')
group_time_filters.add_argument('-y', '--days', type=int, help='Show entries for given days. Gzipped files included')
group_severity = argp.add_mutually_exclusive_group()
group_severity.add_argument('-e', '--errors', action='store_true', default=False, help='Show only entries >= ERROR. Keyword is ignored')
group_severity.add_argument('--severity', help='Show only messages of certain severity level')
# Output
group5 = argp.add_mutually_exclusive_group()
group5.add_argument('-l', '--single-line', action='store_true', help='Join multiline messages to one line')
group5.add_argument('-o', '--short', action='store_true', default=False, help='Show only time, db, user, severity, message, query and trunc latter to 80char')
group_summaries.add_argument('-c', '--conns', action='store_true', default=False, help='Show connection statistics per db/user')
group_summaries.add_argument('--stats', action='store_true', default=False, help='Showing counts per db/severity')
group_summaries.add_argument('--graph', action='store_true', default=False, help='Show a basic graph of event distribution over time. Useful with -e/--errors filter')
# argp.add_argument('--bucket', dest='bucket', default=60, type=int, help='Bucket for aggregations (--graph)')  # TODO non-hourly buckets
argp.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False, help='For debugging')


args = argp.parse_args()
if not args.keyword and not (args.errors or args.people or args.robots or args.user or args.minutes or
                                 args.severity or args.stats or args.conns or args.queries or args.graph):
    argp.print_help()
    exit(1)


logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel((logging.INFO if args.verbose else logging.WARNING))

logger.info('args: %s', args)

outfile = sys.stdout


def get_files_for_days(days, base_glob):
    files = []
    now = datetime.now()
    for i in xrange(days - 1, -1, -1):
        d = now - timedelta(i)
        p = (base_glob + PG_LOG_NAMING).format(curdate=d.strftime('%Y-%m-%d'))
        logging.info('Finding logs from glob: %s', p)
        files.extend(glob.glob(p))
    return files


def filter_out_files_older_than_given_datetime(list_of_filenames, date_constraint):
    i = len(list_of_filenames) - 1  # the last file we need always
    while i > 0:
        if get_log_file_datetime_from_full_name(list_of_filenames[i]) < date_constraint:
            return list_of_filenames[i:]
        i -= 1
    return list_of_filenames[i:]


time_constraint = None
if args.minutes:
    time_constraint = datetime.now() - timedelta(minutes=int(args.minutes))
logging.info("time_constraint = %s", time_constraint)


def get_file_inputs():

    if args.file:
        files = [('args.file', [args.file])]
    elif args.stdin:
        files = [('sys.stdin', [sys.stdin])]
    else:
        if args.days:
            files = get_files_for_days(args.days, args.globpath)
        else:
            glob_path = args.globpath + (PG_LOG_NAMING if args.globpath.find('-') == -1 else '' ).format(curdate=datetime.now().strftime('%Y-%m-%d'))
            logging.info('glob_path: %s', glob_path)
            files = glob.glob(glob_path)    # we assume if '-' is there then full glob with date is provided
        if len(files) == 0:
            return []

        files.sort()
        files_by_instance = defaultdict(list)
        for f in files:
            path, filename = os.path.split(f)
            files_by_instance[path].append(f)
        logging.info("found files: %s", files)
        files = files_by_instance.items()
        files.sort(key=lambda x: x[0])
        if args.minutes and len(files) > 1:
            files = filter_out_files_older_than_given_datetime(files, time_constraint)   # let's remove files definitely older than --minutes

    return files


if args.short:
    args.single_line = True

stats = defaultdict(dict)   # db : {level: count}
conns = defaultdict(dict)   # db: {user: count}
buckets = {}    # {time: count}

matcher_r = re.compile('^(zomcat|tws|nagios|robot|postgres)')
re_remove_ws = re.compile(r'[\t\s\r\n]+')

CSV_FIELDS = [
    'log_time',
    'user_name',
    'database_name',
    'process_id',
    'connection_from',
    'session_id',
    'session_line_num',
    'command_tag',
    'session_start_time',
    'virtual_transaction_id',
    'transaction_id',
    'error_severity',
    'sql_state_code',
    'message',
    'detail',
    'hint',
    'internal_query',
    'internal_query_pos',
    'context',
    'query',
    'query_pos',
    'location',
    'application_name',
]

CSV_FIELDS_SHORT = ['log_time', 'database_name', 'user_name', 'error_severity', 'message', 'query']

csv.field_size_limit(sys.maxsize)
outfile_writer = None
if args.short:
    outfile_writer = csv.DictWriter(outfile, CSV_FIELDS_SHORT)
else:
    outfile_writer = csv.DictWriter(outfile, CSV_FIELDS)

def get_log_file_datetime_from_full_name(full_log_file_path):
    path, filename = os.path.split(full_log_file_path)
    file, ext = os.path.splitext(filename)
    filename = file[file.index('-')+1:]
    # /some/path/postgresql-2013-06-04_095755.csv > 2013-06-04_095755
    return datetime.strptime(filename, '%Y-%m-%d_%H%M%S')

def is_robot_user(username):
    return True if matcher_r.match(username) else False


def line_has_search_kw(line, kw):
    return line['message'].find(kw) != -1 or line['detail'].find(kw) != -1 or line['internal_query'].find(kw) != -1 \
        or line['query'].find(kw) != -1 or line['context'].find(kw) != -1


def is_error_or_greater(severity):
    return severity in ['ERROR', 'FATAL', 'PANIC']


def is_noise(log_entry):
    """
        - pgAdmin3 schema discovery queries
        - connection received/authenticated
        - slow query notifications
    """

    # connection received/authenticated
    if log_entry['error_severity'] == 'LOG':
        if log_entry['message'].startswith('connection received: ') or \
                log_entry['message'].startswith('connection authorized: '):
            return True

    # slow query notifications
    if log_entry['error_severity'] == 'LOG':
        if log_entry['message'].startswith('duration: '):
            return True

    # pgAdmin
    ident = 'pgAdmin III - Browser'
    if log_entry['application_name'].find(ident) >= 0:
        return True
    # These are for some reason done under 'pgAdmin III - Query Tool'
    patterns = ['SELECT format_type(oid', 'SELECT CASE WHEN typbasetype=0 THEN oid else typbasetype END AS basetype', 'SELECT defaclacl FROM pg_catalog.pg_default_acl dacl']
    for p in patterns:  # TODO proper regex or smth even more clever
        if log_entry['message'].find(p) >= 0:
            return True

    return False


def to_datetime(dt_string):
    """ 2014-08-01 09:42:22.343 CEST """
    return datetime.strptime(dt_string, POSTGRES_DATE_FORMAT)


def print_stats(stats, order_by='ERROR'):
    """ stats={'dbname': {level: count, ...}} """
    for db, s in stats.iteritems():
        for level in ['FATAL', 'ERROR', 'WARNING', 'LOG']:
            if level not in s:
                s[level] = 0
    stats = sorted(stats.items(), key=lambda x: x[1][order_by], reverse=True)
    for db, s in stats:
        print '"{0:30}": FATAL {1:7}\tERROR {2:7}\tWARNING {3:7}\tLOG {4:7}'.format(db,
                                                                           s['FATAL'],
                                                                           s['ERROR'],
                                                                           s['WARNING'],
                                                                           s['LOG']
                                                                        )

def print_conns(conns):
    """ conns={'dbname': {user: count, ...}} """
    total = 0

    for db in sorted(conns):
        print '\n---', db, '---'

        for u, c in sorted(conns[db].items(), key=lambda x: x[1], reverse=True):
            print '{0:30}\t{1:6}'.format(u, c)
            total += c

    print '\n--- SUMMARY for timerange {} to {} ---'.format(min_time.strftime('%Y-%m-%d %H:%M'), max_time.strftime('%Y-%m-%d %H:%M'))
    print '{0:30}\t{1:6}'.format('total', total)
    print '{0:30}\t{1:6}'.format('conns/min', round(total / (max_time - min_time).total_seconds() * 60, 1))


def print_graph(buckets):
    """ input: {bucket1: count, bucket2: count}
        output smth like:
--- first bucket start time 2014-12-03 09:00 ---
 -3h: ▇▇▇▇  4
 -2h: ▇▇▇▇▇  5
 -1h: ▇▇▇▇  4
--- last bucket start time 2014-12-04 11:00 ---

    """
    max_width = 60
    tick_char = '▇'
    tick_char_min = '|'
    bucket_count = len(buckets)
    buckets, counts = zip(*sorted(buckets.items(), key=lambda x: x[0]))

    # normalize
    step = 1
    if max(counts) > max_width:
        step = max(counts) / max_width

    print '--- first bucket start time {} ---'.format(buckets[0].strftime('%Y-%m-%d %H:00'))
    for i, count in enumerate(counts):
        ticks = int(count / step)
        if ticks == 0 and count > 0:
            print '{0:3}h: '.format(-(bucket_count-i)) + tick_char_min + '  ' + str(count)
        else:
            print '{0:3}h: '.format(-(bucket_count-i)) + ticks*tick_char + '  ' + str(count)

    print '--- last bucket start time {} ---'.format(buckets[-1].strftime('%Y-%m-%d %H:00'))


def fill_bucket_holes(min_time, buckets):
    first_bucket = min_time.replace(minute=0, second=0, microsecond=0)
    last_bucket = datetime.now().replace(minute=0, second=0, microsecond=0)
    tmp = first_bucket
    ret = {}
    while tmp <= last_bucket:
        ret[tmp] = 0
        tmp += timedelta(hours=1)
    for k, v in buckets.iteritems():
        ret[k] = v
    return ret


def get_bucket(log_time, bucket_width=None):
    return log_time.replace(minute=0, second=0, microsecond=0)


min_time = None     # 1st found log entry time
max_time = None     # last found log entry time


def process_file(fp_in, read_from_position=0):
    """ filters through all found rows and returns last read file position """
    global stats
    global conns
    global buckets
    global min_time
    global max_time

    if read_from_position > 0:
        fp_in.seek(read_from_position)
    reader = csv.DictReader(fp_in, fieldnames=CSV_FIELDS, restval='')
    '''reader: csv.DictReader'''

    for line in reader:

        if line:
            is_error = is_error_or_greater(line['error_severity'])
            user = line['user_name']
            is_robot = is_robot_user(user)

            # logger.info('message: %s', line['message'])
            # logger.info('severity: %s', line['error_severity'])
            # logger.info('user: %s', user)
            # logger.info('is_robot: %s', is_robot)
            # logger.info('is_pgadmin3_noise: %s', is_pgadmin3_noise(line))
            if args.verbose:
                print line

            log_time = to_datetime(line['log_time'])
            if args.minutes:
                if log_time < time_constraint:
                    continue
            if min_time is None or log_time < min_time:
                min_time = log_time
            if max_time is None or log_time > max_time:
                max_time = log_time
            if args.errors and not is_error:
                continue
            if args.people and is_robot:
                continue
            if args.robots and not is_robot:
                continue
            if args.severity and line['error_severity'].upper() != args.severity.upper():
                continue
            if args.dbname and line['database_name'].find(args.dbname) == -1:
                continue
            if args.user and line['user_name'].find(args.user) == -1:
                continue
            if args.queries and not (line['message'].startswith("statement: ") or
                                     line['message'].startswith("duration: ") or
                                     line['message'].startswith("syntax error ") or
                                     line['message'].startswith("execute ") or
                                     line['error_severity'] == 'ERROR'):
                continue
            if args.keyword and not line_has_search_kw(line, args.keyword):
                continue
            if args.no_noise and is_noise(line):
                continue

            if args.stats:
                if line['error_severity'] not in stats[line['database_name']]:
                    stats[line['database_name']][line['error_severity']] = 1
                    continue
                stats[line['database_name']][line['error_severity']] += 1
                continue

            if args.conns:
                if line['command_tag'] == 'authentication':
                    if line['user_name'] not in conns[line['database_name']]:
                        conns[line['database_name']][line['user_name']] = 1
                        continue
                    conns[line['database_name']][line['user_name']] += 1
                continue

            if args.graph:
                bucket = get_bucket(log_time)
                if bucket not in buckets:
                    buckets[bucket] = 1
                else:
                    buckets[bucket] += 1
                continue

            if args.single_line or args.short:
                for field in ['message', 'detail', 'hint', 'internal_query', 'context', 'query']:
                    line[field] = re_remove_ws.sub(' ', line[field])

            if args.short:
                line = {key: value for (key, value) in line.items() if key in CSV_FIELDS_SHORT}
                line['query'] = line['query'][:77] + '...' if len(line['query']) > 77 else line['query']
                line['message'] = line['message'][:77] + '...' if len(line['message']) > 77 else line['message']

            outfile_writer.writerow(line)

    return fp_in.tell()


def process_stdin():
    logging.info('########## processing: stdin ##########')

    if args.tail:
        raise Exception('Tailing stdin not implemented yet!')

    process_file(sys.stdin)


def check_for_next_log_file(last_known_file_full_path):  # could use some filesystem monitoring?
    path = os.path.dirname(last_known_file_full_path)
    listing = glob.glob(os.path.join(path, 'postgresql*.csv'))
    listing.sort(key=os.path.getctime)
    i = listing.index(last_known_file_full_path)
    if len(listing) > 1 and i < len(listing)-1:
        return listing[i+1]
    return None


if __name__ == '__main__':

    try:
        if args.stdin:
            process_stdin()
        else:
            current_file_name = None
            current_file_fp = None
            current_file_pos = None

            for instance_path, instance_files in get_file_inputs():
                logging.info('########## processing: %s ##########', instance_path)

                for f in instance_files:
                    logging.info('### doing input file: %s ###', f)

                    if f.endswith('.gz'):
                        with gzip.open(f, 'rb') as fp:
                            process_file(fp)
                    else:
                        current_file_name = f
                        current_file_fp = open(f)
                        current_file_pos = process_file(current_file_fp)

            if args.tail:
                while True:
                    if current_file_fp:
                        current_file_fp.seek(0, 2)
                        eof_position = current_file_fp.tell()
                    if eof_position > current_file_pos:
                        current_file_pos = process_file(current_file_fp, current_file_pos)
                    elif not args.file:
                        next_file = check_for_next_log_file(current_file_name)
                        if next_file:
                            current_file_name = next_file
                            current_file_fp = open(current_file_name)
                            current_file_pos = process_file(current_file_fp)
                    time.sleep(1)

    except IOError, KeyboardInterrupt:
        pass

    if not min_time:
        print '\n--- no data ---'
        exit(0)

    if args.stats:
        print_stats(stats, order_by='ERROR')
    if args.conns:
        print_conns(conns)
    if args.graph:
        buckets = fill_bucket_holes(min(min_time, time_constraint if time_constraint else datetime.now()), buckets)
        print_graph(buckets)
