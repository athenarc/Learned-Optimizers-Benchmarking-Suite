import psycopg2 as pg
import sys
import os
# Add the project root directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG
from QueryGenerators import *
import toml
# from utils import *
import pickle
import time
import glob
import errno

def make_dir(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def gen_queries(query_template, num_samples):
    '''
    @query_template: dict, or str, as used by QueryGenerator2 or
    QueryGenerator.
    '''
    if isinstance(query_template, dict):
        qg = QueryGenerator2(query_template, DB_CONFIG['user'], DB_CONFIG['host'], DB_CONFIG['port'],
                DB_CONFIG['password'], DB_CONFIG['dbname'])
    elif isinstance(query_template, str):
        qg = QueryGenerator(query_template, DB_CONFIG['user'], DB_CONFIG['host'], DB_CONFIG['port'],
                DB_CONFIG['password'], DB_CONFIG['dbname'])

    gen_sqls = qg.gen_queries(num_samples)
    gen_sqls = remove_doubles(gen_sqls)
    return gen_sqls

def verify_queries(query_strs):
    all_queries = []
    for cur_sql in query_strs:
        start = time.time()
        test_sql = "EXPLAIN " + cur_sql
        output, _ = cached_execute_query(test_sql, DB_CONFIG['user'],
                DB_CONFIG['host'], DB_CONFIG['port'], DB_CONFIG['password'], DB_CONFIG['dbname'],
                100, "./qgen_cache", None)
        if len(output) == 0:
            print("zero query: ", test_sql)
            continue
        else:
            print("query len: {}, time: {}".format(len(output),
                time.time()-start))
        all_queries.append(cur_sql)
    return all_queries

def remove_doubles(query_strs):
    newq = []
    seen_samples = set()
    for q in query_strs:
        if q in seen_samples:
            print(q)
            # pdb.set_trace()
            continue
        seen_samples.add(q)
        newq.append(q)
    return newq

def main():
    from pathlib import Path
    current_file = Path(__file__).resolve()
    repo_root = current_file.parents[2]
    templates_dir = repo_root / "preprocessing" / "query_gen" / "templates"
    generated_queries_base = repo_root / "job_gen" / "query_gen" / "generated_queries"
    fns = list(templates_dir.glob("*.toml"))
    # qdir = "./so_new_queries/"
    SAVE_QUERY_SQLS = True
    # make_dir(qdir)

    for fn in fns:
        start = time.time()
        assert ".toml" in fn
        template_name = os.path.basename(fn).replace(".toml", "")
        out_dir = generated_queries_base / template_name
        make_dir(out_dir)

        template = toml.load(fn)
        query_strs = gen_queries(template, 10)
        query_strs = verify_queries(query_strs)
        query_strs = remove_doubles(query_strs)
        print("after verifying, and removing doubles: ", len(query_strs))

        if SAVE_QUERY_SQLS:
            for i, sql in enumerate(query_strs):
                sql_fn = out_dir + "/" + str(i) + ".sql"
                with open(sql_fn, "w") as f:
                    f.write(sql)

if __name__ == "__main__":
    main()