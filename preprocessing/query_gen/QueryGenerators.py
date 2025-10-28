from utils import *
import random
import time
import pygtrie
import nltk
nltk.download('punkt_tab')
from nltk.tokenize import word_tokenize
import numpy as np
import klepto
import getpass
import pdb
import hashlib
import psycopg2 as pg
import networkx as nx

def deterministic_hash(string):
    return int(hashlib.sha1(str(string).encode("utf-8")).hexdigest(), 16)

def cached_execute_query(sql, user, db_host, port, pwd, db_name,
        execution_cache_threshold, sql_cache_dir=None, timeout=120000):
    '''
    @timeout:
    @db_host: going to ignore it so default localhost is used.
    executes the given sql on the DB, and caches the results in a
    persistent store if it took longer than self.execution_cache_threshold.
    '''
    sql_cache = None
    if sql_cache_dir is not None:
        assert isinstance(sql_cache_dir, str)
        sql_cache = klepto.archives.dir_archive(sql_cache_dir,
                cached=True, serialized=True)

    hashed_sql = deterministic_hash(sql)

    # archive only considers the stuff stored in disk
    if sql_cache is not None and hashed_sql in sql_cache.archive:
        return sql_cache.archive[hashed_sql], False

    start = time.time()

    os_user = getpass.getuser()
    if os_user == "ubuntu":
        # for aws
        con = pg.connect(user=user, port=port,
                password=pwd, database=db_name)
    else:
        # for chunky
        con = pg.connect(user=user, host=db_host, port=port,
                password=pwd, database=db_name)

    cursor = con.cursor()
    if timeout is not None:
        cursor.execute("SET statement_timeout = {}".format(timeout))
    try:
        cursor.execute(sql)
    except Exception as e:
        # print("query failed to execute: ", sql)
        # FIXME: better way to do this.
        cursor.execute("ROLLBACK")
        con.commit()
        cursor.close()
        con.close()

        if not "timeout" in str(e):
            print("failed to execute for reason other than timeout")
            print(e)
            print(sql)
            pdb.set_trace()
        else:
            print("failed because of timeout!")
            return None, True

        return None, False

    exp_output = cursor.fetchall()
    cursor.close()
    con.close()
    end = time.time()
    if (end - start > execution_cache_threshold) \
            and sql_cache is not None:
        sql_cache.archive[hashed_sql] = exp_output
    return exp_output, False

def parse_sql(sql, user, db_name, db_host, port, pwd, timeout=False,
        compute_ground_truth=True, subset_cache_dir="./subset_cache/"):
    '''
    @sql: sql query string.

    @ret: python dict with the keys:
        sql: original sql string
        join_graph: networkX graph representing query and its
        join_edges. Properties include:
            Nodes:
                - table
                - alias
                - predicate matches
            Edges:
                - join_condition

            Note: This is the only place where these strings will be stored.
            Each of the subplans will be represented by their nodes within
            the join_graph, and we can use these properties to reconstruct the
            appropriate query for each subplan.

        subset_graph: networkX graph representing each subplan as a node.

        Properties of each subplan will include all the cardinality data that
        will need to be computed:
            - true_count
            - pg_count
            - total_count
    '''
    start = time.time()
    join_graph = extract_join_graph(sql)
    subset_graph = generate_subset_graph(join_graph)

    print("query has",
          len(join_graph.nodes), "relations,",
          len(join_graph.edges), "joins, and",
          len(subset_graph), " possible subplans.",
          "took:", time.time() - start)

    ret = {}
    ret["sql"] = sql
    ret["join_graph"] = join_graph
    ret["subset_graph"] = subset_graph

    ret["join_graph"] = nx.adjacency_data(ret["join_graph"])
    ret["subset_graph"] = nx.adjacency_data(ret["subset_graph"])
    return ret

class QueryGenerator():
    '''
    Generates sql queries based on a template.
    TODO: explain rules etc.
    '''
    def __init__(self, query_template, user, db_host, port,
            pwd, db_name):
        self.user = user
        self.pwd = pwd
        self.db_host = db_host
        self.port = port
        self.db_name = db_name
        self.query_template = query_template
        # key: column_name, val: [vals]
        self.valid_pred_vals = {}

        # tune-able params
        self.max_in_vals = 15

    def gen_queries(self, num_samples, column_stats=None):
        '''
        @ret: [sql queries]
        '''
        start = time.time()
        # TODO: make these instance variables initialized in __init__
        pred_columns, pred_types, pred_strs = extract_predicates(self.query_template)
        from_clauses, aliases, tables = extract_from_clause(self.query_template)
        joins = extract_join_clause(self.query_template)
        all_query_strs = []

        while len(all_query_strs) < num_samples:
            gen_query = self.query_template
            # now, replace each predicate value 1 by 1
            for i, col in enumerate(pred_columns):
                pred_str = pred_strs[i]
                if pred_types[i] == "eq":
                    pass
                elif pred_types[i] == "in":
                    if not "SELECT" in pred_str[0]:
                        # leave this as is.
                        continue
                    pred_sql = pred_str[0]
                    if col not in self.valid_pred_vals:
                        # pred_sql should be a sql that we can execute
                        output = cached_execute_query(pred_sql, self.user,
                                self.db_host, self.port, self.pwd, self.db_name,
                                100, None, None)
                        self.valid_pred_vals[col] = output

                    min_val = 1
                    # replace pred_sql by a value from the valid ones
                    num_pred_vals = random.randint(min_val, self.max_in_vals)

                    # # find this many values randomly from the given col, and
                    # # update col_vals with it.
                    vals = []
                    for k in range(num_pred_vals):
                        val = random.choice(self.valid_pred_vals[col])
                        if val is not None:
                            # pdb.set_trace()
                            # vals.append("'{}'".format(val[0].replace("'","")))
                            vals.append("'{}'".format(str(val[0]).replace("'","")))
                    # "'{}'".format(random.choice(self.valid_pred_vals[col])[0].replace("'",""))
                    vals = [s for s in set(vals)]
                    vals.sort()
                    new_pred_str = ",".join(vals)
                    gen_query = gen_query.replace("'" + pred_sql + "'", new_pred_str)

                elif pred_types[i] == "lte" or pred_types[i] == "lt":
                    # print("going to sample for range query")
                    # pdb.set_trace()
                    # if not "SELECT" in str(pred_str[0]):
                        # # leave this as is.
                        # continue
                    assert len(pred_str) == 2
                    if col not in self.valid_pred_vals:
                        table = col[0:col.find(".")]
                        if table in aliases:
                            table = ALIAS_FORMAT.format(TABLE = aliases[table],
                                                ALIAS = table)

                        sel_query = SELECT_ALL_COL_TEMPLATE.format(COL = col,
                                                TABLE = table)
                        output = cached_execute_query(sel_query, self.user,
                                self.db_host, self.port, self.pwd, self.db_name,
                                100, None, None)
                        self.valid_pred_vals[col] = output

                    val1 = random.choice(self.valid_pred_vals[col])[0]
                    val2 = random.choice(self.valid_pred_vals[col])[0]
                    low_pred = "X" + col[col.find(".")+1:]
                    high_pred = "Y" + col[col.find(".")+1:]
                    low_val = str(min(val1, val2))
                    high_val = str(max(val1, val2))
                    gen_query = gen_query.replace(low_pred, low_val)
                    gen_query = gen_query.replace(high_pred, high_val)

            # print(gen_query)
            all_query_strs.append(gen_query)
        print("{} query strings took {} seconds to generate".format(len(all_query_strs),
            time.time()-start))
        return all_query_strs


ILIKE_PRED_FMT = "'%{ILIKE_PRED}%'"
class QueryGenerator2():
    '''
    Generates sql queries based on a template.
    TODO: explain rules etc.
    '''
    def __init__(self, query_template, user, db_host, port,
            pwd, db_name):
        self.user = user
        self.pwd = pwd
        self.db_host = db_host
        self.port = port
        self.db_name = db_name
        # self.query_template = query_template
        self.base_sql = query_template["base_sql"]["sql"]
        self.templates = query_template["templates"]
        self.sampling_outputs = {}
        self.ilike_output_size = {}
        self.bad_sqls = []
        self.trie_archive = klepto.archives.dir_archive("./qgen_tries/",
                cached=True, serialized=True)

        self.max_in_vals = 15

    def _update_preds_range(self, sql, column, key, pred_val):
        '''
        '''
        print("key: ", key)
        print("pred_val: ", pred_val)
        pdb.set_trace()
        sql = sql.replace(key, pred_val)
        return sql

    def _generate_sql(self, pred_vals):
        '''
        '''
        sql = self.base_sql
        # for each group, select appropriate predicates
        for key, val in pred_vals.items():
            if key not in sql:
                print("key not in sql!")
                print(key)
                pdb.set_trace()
            sql = sql.replace(key, val)

        return sql

    def _update_sql_ilike(self, ilike_filter, pred_group, pred_vals):
        columns = pred_group["columns"]
        assert len(columns) == 1
        key = pred_group["keys"][0]
        pred_type = pred_group["pred_type"]

        pred_str = columns[0] + " " + pred_type + " " + ilike_filter
        pred_vals[key] = pred_str

    def _update_sql_in(self, samples, pred_group, pred_vals):
        '''
        @samples: ndarray, ith index correspond to possible values for ith
        index of pred_group["keys"], and last index is the count of the
        combined values.

        @pred_vals: all the predicates in the base sql string that have already
        been assigned a value. This will be updated after we assign values to
        the unspecified columns in the present pred_group.

        @pred_group: [[template.predicate]] section of the toml that this
        predicate corresponds to.

        @ret: updated sql string
        '''
        keys = pred_group["keys"]
        columns = pred_group["columns"]
        pred_type = pred_group["pred_type"]
        assert len(keys) == len(columns)

        for i, key in enumerate(keys):
            # key will be replaced with a predicate string
            pred_str = ""
            none_cond = None
            column = columns[i]
            vals = []
            # can have multiple values for IN statements, including None / NULL
            for s in samples:
                val = s[i]
                if val:
                    val = str(val)
                    vals.append("'{}'".format(val.replace("'","")))
                else:
                    # None value
                    none_cond = column + " IS NULL"

            vals = [s for s in set(vals)]
            if len(vals) == 0:
                assert none_cond
                pred_str = none_cond
            else:
                vals.sort()
                new_pred_str = ",".join(vals)
                pred_str = column + " " + pred_type + " "
                pred_str += "(" + new_pred_str + ")"
                if none_cond:
                    pred_str += " OR " + none_cond

            pred_vals[key] = pred_str

    def _gen_query_str(self, templated_preds):
        '''
        @templated_preds

        Modifies the base sql to plug in newer values at all the unspecified
        values.
            Handling of NULLs:

        '''
        # dictionary that is used to keep track of the column values that have
        # already been selected so far.
        pred_vals = {}

        # for each group, select appropriate predicates
        for pred_group in templated_preds:
            if "multi" in pred_group:
                # multiple predicate conditions, choose any one
                pred_group = random.choice(pred_group["multi"])
            if "sql" in pred_group["type"]:
                # cur_sql will be the sql used to sample for this predicate
                # value
                if pred_group["type"] == "sqls":
                    cur_sql = random.choice(pred_group["sqls"])
                else:
                    cur_sql = pred_group["sql"]

                if pred_group["dependencies"]:
                    # need to replace placeholders in cur_sql
                    for key, val in pred_vals.items():
                        cur_sql = cur_sql.replace(key, val)

                # get possible values to use
                cur_key = deterministic_hash(cur_sql)
                if cur_key in self.sampling_outputs:
                    output = self.sampling_outputs[cur_key]
                else:
                    if cur_sql in self.bad_sqls:
                        return None

                    output, _ = cached_execute_query(cur_sql, self.user,
                            self.db_host, self.port, self.pwd, self.db_name,
                            100, "./qgen_cache", None)
                    if pred_group["pred_type"].lower() == "ilike":
                        cur_sql_key = deterministic_hash(cur_sql)
                        if cur_sql_key in self.trie_archive.archive:
                            print(cur_sql)
                            print("found in archive")
                            trie = self.trie_archive.archive[cur_sql_key]
                        else:
                            print("going to tokenize: ", cur_sql)
                            tokens = []
                            for out in output:
                                if out[0] is None:
                                    continue
                                cur_tokens = word_tokenize(out[0].lower())
                                if len(cur_tokens) > 150:
                                    print("too many tokens in column: ",
                                            pred_group["columns"][0], pred_vals)
                                    self.bad_sqls.append(cur_sql)
                                    return None
                                tokens += cur_tokens

                            print("going to make a trie..")
                            trie = pygtrie.CharTrie()
                            for token in tokens:
                                if token in trie:
                                    trie[token] += 1
                                else:
                                    trie[token] = 1
                            self.trie_archive.archive[cur_sql_key] = trie

                        self.sampling_outputs[cur_key] = trie

                        output_keys = []
                        weights = []
                        for k,v in trie.items():
                            output_keys.append(k)
                            weights.append(v)

                        self.ilike_output_size[cur_key] = (output_keys, weights)

                        output = trie
                    else:
                        self.sampling_outputs[cur_key] = output

                if len(output) == 0:
                    # no point in doing shit
                    return None

                if pred_group["pred_type"].lower() == "in":
                    # now use one of the different sampling methods
                    num_samples = random.randint(pred_group["min_samples"],
                            pred_group["max_samples"])

                    if pred_group["sampling_method"] == "quantile":
                        num_quantiles = pred_group["num_quantiles"]
                        curp = random.randint(0, num_quantiles-1)
                        chunk_len = int(len(output) / num_quantiles)
                        tmp_output = output[curp*chunk_len: (curp+1)*chunk_len]
                        if len(tmp_output) == 0:
                            # really shouldn't be happenning right?
                            return None

                        if len(tmp_output) <= num_samples:
                            samples = [random.choice(tmp_output) for _ in
                                    range(num_samples)]
                        else:
                            samples = random.sample(tmp_output, num_samples)

                        self._update_sql_in(samples,
                                pred_group, pred_vals)

                    else:
                        samples = [random.choice(output) for _ in
                                range(num_samples)]
                        self._update_sql_in(samples,
                                pred_group, pred_vals)

                elif pred_group["pred_type"].lower() in ["ilike", "not ilike"]:
                    assert isinstance(output, pygtrie.CharTrie)

                    # Note: the trie will only provide a lower-bound on the
                    # number of matches, since ILIKE predicates would also
                    # consider substrings. But this seems to be enough for our
                    # purposes, as we will avoid queries that zero out
                    output_keys, weights = self.ilike_output_size[cur_key]
                    if len(output_keys) <= 1:
                        return None

                    # choose min_target, max_target for regex matches.
                    if "thresholds" in pred_group:
                        threshs = pred_group["thresholds"]
                        idx = random.randint(0, len(threshs)-1)
                        min_target = threshs[idx]
                        if idx+1 == len(threshs):
                            max_target = 100000000000
                        else:
                            max_target = threshs[idx+1]
                    else:
                        if random.random() > 0.5:
                            cur_partition = random.randint(0, pred_group["num_quantiles"]-1)
                            min_percentile = 100.0 / pred_group["num_quantiles"] * cur_partition
                            max_percentile = 100.0 / pred_group["num_quantiles"] * (cur_partition+1)
                            min_target = max(pred_group["min_count"],
                                    np.percentile(weights, min_percentile))
                            max_target = np.percentile(weights, max_percentile)
                        else:
                            num_rows = sum(weights)
                            partition_size = num_rows / pred_group["num_quantiles"]
                            cur_partition = random.randint(0, pred_group["num_quantiles"]-1)
                            min_target = max(pred_group["min_count"],
                                    cur_partition*partition_size)
                            max_target = (cur_partition+1)*partition_size

                            cur_partition = random.randint(0, pred_group["num_quantiles"]-1)

                            min_percentile = 100.0 / pred_group["num_quantiles"] * cur_partition
                            max_percentile = 100.0 / pred_group["num_quantiles"] * (cur_partition+1)
                            min_target = max(pred_group["min_count"],
                                    np.percentile(weights, min_percentile))
                            max_target = np.percentile(weights, max_percentile)

                    if min_target > max_target:
                        print("min target {} > max target {}".format(min_target,
                                    max_target))
                        return None

                    print("col: {}, min: {}, max: {}".format(pred_group["columns"],
                        min_target, max_target))

                    ilike_pred = None
                    for i in range(1000):
                        i += 1
                        if i % 1000 == 0:
                            print(i)
                        if i % 2 == 0:
                            # just choose randomly, more likely to find
                            # relevant one
                            key = random.choice(output_keys)
                        else:
                            key = random.choices(population=output_keys,
                                    weights=weights, k=1)[0]
                        if len(key) < pred_group["min_chars"]:
                            continue

                        max_filter_len = min(len(key), pred_group["max_chars"])
                        num_chars = random.randint(pred_group["min_chars"],
                                max_filter_len)
                        ilike_pred = key[0:num_chars]
                        est_size = sum(output[ilike_pred:])
                        if est_size > min_target and est_size < max_target:
                            break
                        else:
                            ilike_pred = None

                    if ilike_pred is None:
                        # print("did not find an appropriate predicate for ",
                                # pred_group["columns"])
                        return None
                    else:
                        print("col: {}, filter: {}, est size: {}".format(
                            pred_group["columns"][0], ilike_pred, est_size))
                    ilike_pred = ilike_pred.replace("'","")

                    if pred_group["pred_type"].lower() == "ilike":
                        ilike_filter = ILIKE_PRED_FMT.format(ILIKE_PRED = ilike_pred)
                    else:
                        # Handle NOT ILIKE
                        ilike_filter = "NOT " + ILIKE_PRED_FMT.format(ILIKE_PRED = ilike_pred)

                    self._update_sql_ilike(ilike_filter, pred_group, pred_vals)
                else:
                    assert False

            elif pred_group["type"] == "list":
                ## assuming it is a single column
                columns = pred_group["columns"]
                assert len(columns) == 1
                if pred_group["sampling_method"] == "uniform":
                    if pred_group["pred_type"] == "range":
                        col = columns[0]
                        assert len(pred_group["keys"]) == 2
                        options = pred_group["options"]
                        pred_choice = random.choice(options)
                        assert len(pred_choice) == 2
                        lower_key = pred_group["keys"][0]
                        upper_key = pred_group["keys"][1]
                        lower_val = pred_choice[0]
                        upper_val = pred_choice[1]

                        assert len(pred_choice) == 2
                        if "numeric_col_type" in pred_group:
                            col_type = pred_group["numeric_col_type"]
                            # add a chec for both conditions
                            float_regex = '^(?:[1-9]\d*|0)?(?:\.\d+)?$'
                            num_check_cond_tmp = "{col} ~ '{regex}' AND {cond}"

                            upper_cond = "{val} <= {col}::{col_type}".format(col=col,
                                                                val=lower_val,
                                                                col_type=col_type)
                            lower_cond = "{col}::{col_type} <= {val}".format(col=col,
                                                                val=upper_val,
                                                                col_type=col_type)
                            lower_cond = num_check_cond_tmp.format(col=col,
                                                        cond = lower_cond,
                                                        regex = float_regex)
                            upper_cond = num_check_cond_tmp.format(col=col,
                                                    cond = upper_cond, regex =
                                                    float_regex)
                        else:
                            lower_key = pred_group["keys"][0]
                            upper_key = pred_group["keys"][1]
                            lower_val = pred_choice[0]
                            upper_val = pred_choice[1]
                            lower_cond = "{} >= {}".format(col, lower_val)
                            upper_cond = "{} <= {}".format(col, upper_val)

                        pred_vals[lower_key] = lower_cond
                        pred_vals[upper_key] = upper_cond

                    else:
                        options = pred_group["options"]
                        pred_choice = random.choice(options)
                        if "replace" in pred_group:
                            # assert len(pred_choice) == 1
                            assert len(pred_group["keys"]) == 1
                            # cur_choice = pred_choice[0]
                            cur_key = pred_group["keys"][0]
                            pred_vals[cur_key] = pred_choice
                            pdb.set_trace()
                        else:
                            # probably only deals with `=` ?
                            assert len(pred_group["keys"]) == 1
                            self._update_sql_in([[pred_choice]],
                                    pred_group, pred_vals)
            else:
                assert False

        gen_sql = self._generate_sql(pred_vals)
        return gen_sql

    def gen_queries(self, num_samples, column_stats=None):
        '''
        @ret: [sql queries]
        '''
        print("going to generate ", num_samples)
        start = time.time()
        all_query_strs = []

        while len(all_query_strs) < num_samples:
            for template in self.templates:
                query_str = self._gen_query_str(template["predicates"])
                if query_str is not None:
                    all_query_strs.append(query_str)
                    print(query_str)
                    # pdb.set_trace()
                else:
                    pass
                    # print("query str was None")

        print("{} took {} seconds to generate".format(len(all_query_strs),
            time.time()-start))
        return all_query_strs