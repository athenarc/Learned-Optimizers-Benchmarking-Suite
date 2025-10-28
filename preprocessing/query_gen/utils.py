import networkx as nx
from moz_sql_parser import parse
import pdb
import time
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
from ctypes import *
import itertools

SAMPLE_TABLES = ["title", "name", "aka_name", "keyword", "movie_info",
        "movie_companies", "company_type", "kind_type", "info_type",
        "role_type", "company_name"]

SOURCE_NODE = tuple("s")
SOURCE_NODE_CONST = 100000
OLD_TIMEOUT_COUNT_CONSTANT = 150001001
OLD_CROSS_JOIN_CONSTANT = 150001000
OLD_EXCEPTION_COUNT_CONSTANT = 150001002

TIMEOUT_COUNT_CONSTANT = 150001000001
CROSS_JOIN_CONSTANT = 150001000000
EXCEPTION_COUNT_CONSTANT = 150001000002

CROSS_JOIN_CARD = 19329323

CREATE_TABLE_TEMPLATE = "CREATE TABLE {name} (id SERIAL, {columns})"
INSERT_TEMPLATE = "INSERT INTO {name} ({columns}) VALUES %s"

NTILE_CLAUSE = "ntile({BINS}) OVER (ORDER BY {COLUMN}) AS {ALIAS}"
GROUPBY_TEMPLATE = "SELECT {COLS}, COUNT(*) FROM {FROM_CLAUSE} GROUP BY {COLS}"
COUNT_SIZE_TEMPLATE = "SELECT COUNT(*) FROM {FROM_CLAUSE}"

SELECT_ALL_COL_TEMPLATE = "SELECT {COL} FROM {TABLE} WHERE {COL} IS NOT NULL"
ALIAS_FORMAT = "{TABLE} AS {ALIAS}"
MIN_TEMPLATE = "SELECT {COL} FROM {TABLE} WHERE {COL} IS NOT NULL ORDER BY {COL} ASC LIMIT 1"
MAX_TEMPLATE = "SELECT {COL} FROM {TABLE} WHERE {COL} IS NOT NULL ORDER BY {COL} DESC LIMIT 1"
UNIQUE_VALS_TEMPLATE = "SELECT DISTINCT {COL} FROM {FROM_CLAUSE}"
UNIQUE_COUNT_TEMPLATE = "SELECT COUNT(*) FROM (SELECT DISTINCT {COL} from {FROM_CLAUSE}) AS t"

INDEX_LIST_CMD = """
select
    t.relname as table_name,
    a.attname as column_name,
    i.relname as index_name
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
where
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
order by
    t.relname,
    i.relname;"""


RANGE_PREDS = ["gt", "gte", "lt", "lte"]

CREATE_INDEX_TMP = '''CREATE INDEX IF NOT EXISTS {INDEX_NAME} ON {TABLE} ({COLUMN});'''

NODE_COLORS = {}
# NODE_COLORS["Hash Join"] = 'b'
# NODE_COLORS["Merge Join"] = 'r'
# NODE_COLORS["Nested Loop"] = 'c'

NODE_COLORS["Index Scan"] = 'b'
NODE_COLORS["Seq Scan"] = 'r'
NODE_COLORS["Bitmap Heap Scan"] = 'c'

# figure this out...
NODE_COLORS["Hash"] = 'b'
NODE_COLORS["Materialize"] = 'w'
NODE_COLORS["Sort"] = 'b'

# for signifying whether the join was a left join or right join
EDGE_COLORS = {}
EDGE_COLORS["left"] = "b"
EDGE_COLORS["right"] = "r"

NILJ_CONSTANT = 0.001
NILJ_CONSTANT2 = 2.0
SEQ_CONST = 20.0
RATIO_MUL_CONST = 1.0
NILJ_MIN_CARD = 5.0
CARD_DIVIDER = 0.001
INDEX_COST_CONSTANT = 10000
INDEX_PENALTY_MULTIPLE = 10.0

def extract_predicates(query):
    '''
    @ret:
        - column names with predicate conditions in WHERE.
        - predicate operator type (e.g., "in", "lte" etc.)
        - predicate value
    Note: join conditions don't count as predicate conditions.

    FIXME: temporary hack. For range queries, always returning key
    "lt", and vals for both the lower and upper bound
    '''
    def parse_column(pred, cur_pred_type):
        '''
        gets the name of the column, and whether column location is on the left
        (0) or right (1)
        '''
        for i, obj in enumerate(pred[cur_pred_type]):
            assert i <= 1
            if isinstance(obj, str) and "." in obj:
                # assert "." in obj
                column = obj
            elif isinstance(obj, dict):
                assert "literal" in obj
                val = obj["literal"]
                val_loc = i
            else:
                val = obj
                val_loc = i

        assert column is not None
        assert val is not None
        return column, val_loc, val

    def _parse_predicate(pred, pred_type):
        if pred_type == "eq":
            columns = pred[pred_type]
            if len(columns) <= 1:
                return None
            # FIXME: more robust handling?
            if "." in str(columns[1]):
                # should be a join, skip this.
                # Note: joins only happen in "eq" predicates
                return None
            predicate_types.append(pred_type)
            predicate_cols.append(columns[0])
            predicate_vals.append(columns[1])

        elif pred_type in RANGE_PREDS:
            vals = [None, None]
            col_name, val_loc, val = parse_column(pred, pred_type)
            vals[val_loc] = val

            # this loop may find no matching predicate for the other side, in
            # which case, we just leave the val as None
            for pred2 in pred_vals:
                pred2_type = list(pred2.keys())[0]
                if pred2_type in RANGE_PREDS:
                    col_name2, val_loc2, val2 = parse_column(pred2, pred2_type)
                    if col_name2 == col_name:
                        # assert val_loc2 != val_loc
                        if val_loc2 == val_loc:
                            # same predicate as pred
                            continue
                        vals[val_loc2] = val2
                        break

            predicate_types.append("lt")
            predicate_cols.append(col_name)
            if "g" in pred_type:
                # reverse vals, since left hand side now means upper bound
                vals.reverse()
            predicate_vals.append(vals)

        elif pred_type == "between":
            # we just treat it as a range query
            col = pred[pred_type][0]
            val1 = pred[pred_type][1]
            val2 = pred[pred_type][2]
            vals = [val1, val2]
            predicate_types.append("lt")
            predicate_cols.append(col)
            predicate_vals.append(vals)
        elif pred_type == "in" \
                or "like" in pred_type:
            # includes preds like, ilike, nlike etc.
            column = pred[pred_type][0]
            # what if column has been seen before? Will just be added again to
            # the list of predicates, which is the correct behaviour
            vals = pred[pred_type][1]
            if isinstance(vals, dict):
                vals = vals["literal"]
            if not isinstance(vals, list):
                vals = [vals]
            predicate_types.append(pred_type)
            predicate_cols.append(column)
            predicate_vals.append(vals)
        elif pred_type == "or":
            for pred2 in pred[pred_type]:
                # print(pred2)
                assert len(pred2.keys()) == 1
                pred_type2 = list(pred2.keys())[0]
                _parse_predicate(pred2, pred_type2)

        elif pred_type == "missing":
            column = pred[pred_type]
            val = ["NULL"]
            predicate_types.append("in")
            predicate_cols.append(column)
            predicate_vals.append(val)
        else:
            # assert False
            # TODO: need to support "OR" statements
            return None
            # assert False, "unsupported predicate type"

    start = time.time()
    predicate_cols = []
    predicate_types = []
    predicate_vals = []
    if "::float" in query:
        query = query.replace("::float", "")
    elif "::int" in query:
        query = query.replace("::int", "")
    # really fucking dumb
    bad_str1 = "mii2.info ~ '^(?:[1-9]\d*|0)?(?:\.\d+)?$' AND"
    bad_str2 = "mii1.info ~ '^(?:[1-9]\d*|0)?(?:\.\d+)?$' AND"
    if bad_str1 in query:
        query = query.replace(bad_str1, "")

    if bad_str2 in query:
        query = query.replace(bad_str2, "")

    # FIXME: temporary workaround moz_sql_parser...
    query = query.replace("ILIKE", "LIKE")
    try:
        parsed_query = parse(query)
    except:
        print(query)
        print("moz sql parser failed to parse this!")
        pdb.set_trace()
    pred_vals = get_all_wheres(parsed_query)

    for i, pred in enumerate(pred_vals):
        try:
            assert len(pred.keys()) == 1
        except:
            print(pred)
            pdb.set_trace()
        pred_type = list(pred.keys())[0]
        # if pred == "or" or pred == "OR":
            # continue
        _parse_predicate(pred, pred_type)

    # print("extract predicate cols done!")
    # print("extract predicates took ", time.time() - start)
    return predicate_cols, predicate_types, predicate_vals

def get_all_wheres(parsed_query):
    pred_vals = []
    if "where" not in parsed_query:
        pass
    elif "and" not in parsed_query["where"]:
        pred_vals = [parsed_query["where"]]
    else:
        pred_vals = parsed_query["where"]["and"]
    return pred_vals


def extract_from_clause(query):
    '''
    Optimized version using sqlparse.
    Extracts the from statement, and the relevant joins when there are multiple
    tables.
    @ret: froms:
          froms: [alias1, alias2, ...] OR [table1, table2,...]
          aliases:{alias1: table1, alias2: table2} (OR [] if no aliases present)
          tables: [table1, table2, ...]
    '''
    def handle_table(identifier):
        table_name = identifier.get_real_name()
        alias = identifier.get_alias()
        tables.append(table_name)
        if alias is not None:
            from_clause = ALIAS_FORMAT.format(TABLE = table_name,
                                ALIAS = alias)
            froms.append(from_clause)
            aliases[alias] = table_name
        else:
            froms.append(table_name)

    start = time.time()
    froms = []
    # key: alias, val: table name
    aliases = {}
    # just table names
    tables = []

    start = time.time()
    parsed = sqlparse.parse(query)[0]
    # let us go over all the where clauses
    from_token = None
    from_seen = False
    for token in parsed.tokens:
        if from_seen:
            if isinstance(token, IdentifierList) or isinstance(token,
                    Identifier):
                from_token = token
                break
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True
    assert from_token is not None
    if isinstance(from_token, IdentifierList):
        for identifier in from_token.get_identifiers():
            handle_table(identifier)
    elif isinstance(from_token, Identifier):
        handle_table(from_token)
    else:
        assert False

    return froms, aliases, tables

def connected_subgraphs(g):
    # for i in range(2, len(g)+1):
    for i in range(1, len(g)+1):
        for nodes_in_sg in itertools.combinations(g.nodes, i):
            sg = g.subgraph(nodes_in_sg)
            if nx.is_connected(sg):
                yield tuple(sorted(sg.nodes))

def generate_subset_graph(g):
    subset_graph = nx.DiGraph()
    for csg in connected_subgraphs(g):
        subset_graph.add_node(csg)

    # group by size
    max_subgraph_size = max(len(x) for x in subset_graph.nodes)
    subgraph_groups = [[] for _ in range(max_subgraph_size)]
    for node in subset_graph.nodes:
        subgraph_groups[len(node)-1].append(node)

    for g1, g2 in zip(subgraph_groups, subgraph_groups[1:]):
        for superset in g2:
            super_as_set = set(superset)
            for subset in g1:
                assert len(superset) == len(subset) + 1
                if set(subset) < super_as_set:
                    subset_graph.add_edge(superset, subset)

    return subset_graph

def find_all_tables_till_keyword(token):
    tables = []
    # print("fattk: ", token)
    index = 0
    while (True):
        if (type(token) == sqlparse.sql.Comparison):
            left = token.left
            right = token.right
            if (type(left) == sqlparse.sql.Identifier):
                tables.append(left.get_parent_name())
            if (type(right) == sqlparse.sql.Identifier):
                tables.append(right.get_parent_name())
            break
        elif (type(token) == sqlparse.sql.Identifier):
            tables.append(token.get_parent_name())
            break
        try:
            index, token = token.token_next(index)
            if ("Literal" in str(token.ttype)) or token.is_keyword:
                break
        except:
            break

    return tables

def find_next_match(tables, wheres, index):
    '''
    ignore everything till next
    '''
    match = ""
    _, token = wheres.token_next(index)
    if token is None:
        return None, None
    # FIXME: is this right?
    if token.is_keyword:
        index, token = wheres.token_next(index)

    tables_in_pred = find_all_tables_till_keyword(token)
    assert len(tables_in_pred) <= 2

    token_list = sqlparse.sql.TokenList(wheres)

    while True:
        # Ensure index is within bounds
        if index >= len(token_list.tokens):
            break
        
        index, token = token_list.token_next(index)
        if token is None:
            break

        # print("token.value: ", token.value)
        if token.value.upper() == "AND":
            break

        match += " " + token.value

        if (token.value.upper() == "BETWEEN"):
            # ugh ugliness
            index, a = token_list.token_next(index)
            index, AND = token_list.token_next(index)
            index, b = token_list.token_next(index)
            match += " " + a.value
            match += " " + AND.value
            match += " " + b.value
            # Note: important not to break here! Will break when we hit the
            # "AND" in the next iteration.

    # print("tables: ", tables)
    # print("match: ", match)
    # print("tables in pred: ", tables_in_pred)
    for table in tables_in_pred:
        if table not in tables:
            # print(tables)
            # print(table)
            # pdb.set_trace()
            # print("returning index, None")
            return index, None

    if len(tables_in_pred) == 0:
        return index, None

    return index, match

def find_all_clauses(tables, wheres):
    matched = []
    # print(tables)
    index = 0
    while True:
        index, match = find_next_match(tables, wheres, index)
        # print("got index, match: ", index)
        # print(match)
        if match is not None:
            matched.append(match)
        if index is None:
            break

    return matched

def extract_join_graph(sql):
    '''
    @sql: string
    '''
    froms,aliases,tables = extract_from_clause(sql)
    joins = extract_join_clause(sql)
    join_graph = nx.Graph()

    for j in joins:
        j1 = j.split("=")[0]
        j2 = j.split("=")[1]
        t1 = j1[0:j1.find(".")].strip()
        t2 = j2[0:j2.find(".")].strip()
        try:
            assert t1 in tables or t1 in aliases
            assert t2 in tables or t2 in aliases
        except:
            print(t1, t2)
            print(tables)
            print(joins)
            print("table not in tables!")
            pdb.set_trace()

        join_graph.add_edge(t1, t2)
        join_graph[t1][t2]["join_condition"] = j
        if t1 in aliases:
            table1 = aliases[t1]
            table2 = aliases[t2]

            join_graph.nodes()[t1]["real_name"] = table1
            join_graph.nodes()[t2]["real_name"] = table2

    parsed = sqlparse.parse(sql)[0]
    # let us go over all the where clauses
    where_clauses = None
    for token in parsed.tokens:
        if (type(token) == sqlparse.sql.Where):
            where_clauses = token
    assert where_clauses is not None

    for t1 in join_graph.nodes():
        tables = [t1]
        matches = find_all_clauses(tables, where_clauses)
        join_graph.nodes()[t1]["predicates"] = matches

    return join_graph

def extract_join_clause(query):
    '''
    FIXME: this can be optimized further / or made to handle more cases
    '''
    parsed = sqlparse.parse(query)[0]
    # let us go over all the where clauses
    start = time.time()
    where_clauses = None
    for token in parsed.tokens:
        if (type(token) == sqlparse.sql.Where):
            where_clauses = token
    if where_clauses is None:
        return []
    join_clauses = []

    froms, aliases, table_names = extract_from_clause(query)
    if len(aliases) > 0:
        tables = [k for k in aliases]
    else:
        tables = table_names
    matches = find_all_clauses(tables, where_clauses)

    for match in matches:
        if "=" not in match or match.count("=") > 1:
            continue
        if "<=" in match or ">=" in match:
            continue
        match = match.replace(";", "")
        if "!" in match:
            left, right = match.split("!=")
            if "." in right:
                # must be a join, so add it.
                join_clauses.append(left.strip() + " != " + right.strip())
            continue
        left, right = match.split("=")
        # ugh dumb hack
        if "." in right:
            # must be a join, so add it.
            join_clauses.append(left.strip() + " = " + right.strip())

    return join_clauses