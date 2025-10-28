# Copyright 2022 The Balsa Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import collections
import contextlib
from select import select
import socket
# lehl@2022-10-19: Additional libraries to perform query logging
import os
import time

import psycopg2
import psycopg2.extensions
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE
import ray
import sqlalchemy

import os
from dotenv import load_dotenv

def load_repo_env():
    """
    Find and load the .env file located in the Learned-Optimizers-Benchmarking-Suite repo root,
    regardless of where this script is executed from.
    """
    current_dir = os.path.abspath(os.path.dirname(__file__))

    while True:
        # Check if we’ve reached the filesystem root
        parent_dir = os.path.dirname(current_dir)
        if current_dir == parent_dir:
            raise FileNotFoundError("Could not find .env file in any parent directory.")
        
        env_path = os.path.join(current_dir, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
            print(f"✅ Loaded environment variables from: {env_path}")
            break

        current_dir = parent_dir  # go up one directory

# Load environment variables from the repo root
load_repo_env()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
IMDB_PORT = os.getenv("IMDB_PORT", "5432")

# Change these strings so that psycopg2.connect(dsn=dsn_val) works correctly
# for local & remote Postgres.

# JOB/IMDB.
LOCAL_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{IMDB_PORT}/imdbload"
REMOTE_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{IMDB_PORT}/imdbload"

# TPC-H
# LOCAL_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{TPCH_PORT}/tpch"
# REMOTE_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{TPCH_PORT}/tpch"

# TPC-DS
# LOCAL_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{TPCDS_PORT}/tpcds"
# REMOTE_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{TPCDS_PORT}/tpcds"

# SSB
# LOCAL_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{SSB_PORT}/ssb"
# REMOTE_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{SSB_PORT}/ssb"

# STACK 2011
# LOCAL_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{STACK_PORT}/stack_2011"
# REMOTE_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{STACK_PORT}/stack_2011"

# STACK 2015
# LOCAL_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{STACK_PORT}/stack_2015"
# REMOTE_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{STACK_PORT}/stack_2015"

# STACK 2019
# LOCAL_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{STACK_PORT}/stack_2019"
# REMOTE_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{STACK_PORT}/stack_2019"

local_engine = sqlalchemy.create_engine(
    LOCAL_DSN,
    isolation_level="AUTOCOMMIT",
    pool_pre_ping=True,
)

remote_engine = sqlalchemy.create_engine(
    REMOTE_DSN,
    isolation_level="AUTOCOMMIT",
    pool_pre_ping=True,
)

QUERY_LOG_FILE = 'query_log_file.txt'
NUM_EXECUTIONS = 3

# A simple class holding an execution result.
#   result: a list, outputs from cursor.fetchall().  E.g., the textual outputs
#     from EXPLAIN ANALYZE.
#   has_timeout: bool, set to True iff an execution has run outside of its
#     allocated timeouts; False otherwise (e.g., for non-execution statements
#     such as EXPLAIN).
#   server_ip: str, the private IP address of the Postgres server that
#     generated this Result.
Result = collections.namedtuple(
    'Result',
    ['result', 'has_timeout', 'server_ip'],
)

# ----------------------------------------
#     Psycopg setup
# ----------------------------------------


# When the driver program (e.g. run.py) is interrupted
# the DB will stop executing in-flight queries peacefully.
# Ref: https://www.psycopg.org/articles/2014/07/20/cancelling-postgresql-statements-python/
def wait_select_inter(conn):
    while 1:
        try:
            state = conn.poll()
            if state == POLL_OK:
                break
            elif state == POLL_READ:
                select([conn.fileno()], [], [])
            elif state == POLL_WRITE:
                select([], [conn.fileno()], [])
            else:
                raise conn.OperationalError("bad state from poll: %s" % state)
        except KeyboardInterrupt:
            conn.cancel()
            # the loop will be broken by a server error
            continue


# COMMENTED OUT -> Likely causing errors for queries where the database state breaks,
# disabling as it is not necessary for single-machine/-threaded experiments
#
# psycopg2.extensions.set_wait_callback(wait_select_inter)

@contextlib.contextmanager
def Cursor(dsn=LOCAL_DSN):
    """
    Get a cursor from a pooled SQLAlchemy connection.

    This context manager checks out a connection from a pre-configured pool,
    provides a DBAPI cursor, and ensures the connection is returned to the
    pool when done.
    """
    # Select the appropriate engine based on the DSN.
    engine = local_engine if dsn == LOCAL_DSN else remote_engine
    
    with engine.connect() as connection:
        # Get the underlying psycopg2 connection object to maintain compatibility.
        if hasattr(connection, 'raw_connection'):
            raw_conn = connection.raw_connection()
        else:
            raw_conn = connection.connection
        try:
            with raw_conn.cursor() as cursor:
                # Load pg_hint_plan for the session, same as the original implementation.
                cursor.execute("load 'pg_hint_plan';")
                yield cursor
        finally:
            # The 'with engine.connect() ...' block handles returning the
            # connection to the pool automatically. The raw_conn does not
            # need to be closed separately.
            pass


# ----------------------------------------
#     Postgres execution
# ----------------------------------------


def _SetGeneticOptimizer(flag, cursor):
    # NOTE: DISCARD would erase settings specified via SET commands.  Make sure
    # no DISCARD ALL is called unexpectedly.
    assert cursor is not None
    assert flag in ['on', 'off', 'default'], flag
    cursor.execute('set geqo = {};'.format(flag))
    assert cursor.statusmessage == 'SET'


def ExecuteRemote(sql, verbose=False, geqo_off=False, timeout_ms=None, file_prefix=''):
    return _ExecuteRemoteImpl.remote(sql, verbose, geqo_off, timeout_ms, file_prefix=file_prefix)


@ray.remote(resources={'pg': 1})
def _ExecuteRemoteImpl(sql, verbose, geqo_off, timeout_ms, file_prefix=''):
    with Cursor(dsn=REMOTE_DSN) as cursor:
        return Execute(sql, verbose, geqo_off, timeout_ms, cursor, file_prefix=file_prefix)


def Execute(sql, verbose=False, geqo_off=False, timeout_ms=None, cursor=None, file_prefix=''):
    """Executes a sql statement.

    Returns:
      A pg_executor.Result.
    """
    if verbose:
        print(sql)

    _SetGeneticOptimizer('off' if geqo_off else 'on', cursor)
    if timeout_ms is not None:
        cursor.execute('SET statement_timeout to {}'.format(int(timeout_ms)))
    else:
        # Specifically setting a higher than default timeout so that
        # Neo does not time out for very long running STACK queries
        #
        if LOCAL_DSN.endswith('so'):
            cursor.execute(f'SET statement_timeout to {5 * 60 * 1000}')
        else:
            # Passing None / setting to 0 means disabling timeout.
            cursor.execute('SET statement_timeout to 0')
    try:
        # for iteration in range(NUM_EXECUTIONS):
        cursor.execute(sql)
        result = cursor.fetchall()
        has_timeout = False
    except Exception as e:
        if isinstance(e, psycopg2.errors.QueryCanceled):
            assert 'canceling statement due to statement timeout'\
                in str(e).strip(), e
            result = []
            has_timeout = True
        elif isinstance(e, psycopg2.errors.InternalError_):
            print(
                'psycopg2.errors.InternalError_, treating as a'\
                ' timeout'
            )
            print(e)
            result = []
            has_timeout = True
        elif isinstance(e, psycopg2.OperationalError):
            # This usually indicates an expensive query, putting the server
            # into recovery mode.  'cursor' may get closed too.
            print('Treating as a timeout:', e)
            result = []
            has_timeout = True
        
            if 'the database system is in recovery mode' in str(e).strip():
                time.sleep(10)

            # COMMENTED OUT, since it leads unrecoverable errors in case the database
            # goes down shortly because of an erroneous query
            #
            # if 'SSL SYSCALL error: EOF detected' in str(e).strip():
            #     # This usually indicates an expensive query, putting the server
            #     # into recovery mode.  'cursor' may get closed too.
            #     print('Treating as a timeout:', e)
            #     result = []
            #     has_timeout = True
            # else:
            #     # E.g., psycopg2.OperationalError: FATAL: the database system
            #     # is in recovery mode
            #     raise e
        else:
            raise e
    else:
        # Add logging of executed queries
        #
        try:
            latency = float(result[0][0][0]['Execution Time'])
            planning_time = float(result[0][0][0]['Planning Time'])
        except IndexError:
            pass
        except KeyError:
            pass
        else:
            query_log_file_name = "_".join([file_prefix, QUERY_LOG_FILE])

            with open(query_log_file_name, 'a') as qlf:
                qlf.write(";".join([str(sql).replace('\n',' '), str(latency), str(planning_time), str(timeout_ms), str(geqo_off)]))
                qlf.write(os.linesep)

            print(f"Executed Query on Postgres taking {latency:.3f} ({planning_time:.3f} planning).")

    try:
        _SetGeneticOptimizer('default', cursor)
    except psycopg2.InterfaceError as e:
        # This could happen if the server is in recovery, due to some expensive
        # queries just crashing the server (see the above exceptions).
        assert 'cursor already closed' in str(e), e
        pass
    ip = socket.gethostbyname(socket.gethostname())
    return Result(result, has_timeout, ip)
