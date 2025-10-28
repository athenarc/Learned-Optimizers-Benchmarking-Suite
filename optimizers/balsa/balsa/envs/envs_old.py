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
"""Workload definitions."""
import glob
import os

import numpy as np

import balsa
from balsa import hyperparams
from balsa.util import plans_lib
from balsa.util import postgres
from tqdm import tqdm

_EPSILON = 1e-6


def ParseSqlToNode(path):
    base = os.path.basename(path)
    query_name = os.path.splitext(base)[0]
    with open(path, 'r') as f:
        sql_string = f.read()
    node, json_dict = postgres.SqlToPlanNode(sql_string)
    node.info['path'] = path
    node.info['sql_str'] = sql_string
    node.info['query_name'] = query_name
    node.info['explain_json'] = json_dict
    node.GetOrParseSql()
    return node


class Workload(object):

    @classmethod
    def Params(cls):
        p = hyperparams.InstantiableParams(cls)
        p.Define('query_dir', None, 'Directory to workload queries.')
        p.Define('test_query_dir', None,
                 'Directory to workload test queries. If None, use query_dir.')
        p.Define(
            'query_glob', '*.sql',
            'If supplied, glob for this pattern.  Otherwise, use all queries.'\
            '  Example: 29*.sql.'
        )
        p.Define(
            'workload_order_file', None,
            'Text file specifying the order of queries for training.'
        )
        p.Define(
            'loop_through_queries', False,
            'Loop through a random permutation of queries? '
            'Desirable for evaluation.')
        p.Define(
            'test_query_glob', None,
            'Similar usage as query_glob. If None, treating all queries'\
            ' as training nodes.'
        )
        p.Define('search_space_join_ops',
                 ['Hash Join', 'Merge Join', 'Nested Loop'],
                 'Join operators to learn.')
        p.Define('search_space_scan_ops',
                 ['Index Scan', 'Index Only Scan', 'Seq Scan', 'Bitmap Heap Scan', 'Tid Scan'],
                 'Scan operators to learn.')
        p.Define('skip_neo_processed', False,
                 'Skip queries that already have a NEO directory.')
        p.Define('skip_balsa_processed', False,
                 'Skip queries that already have a Balsa directory.')
        p.Define('require_loger_dir', False,
                 'Require a loger_dir to be included in the parameters.')
        return p

    def __init__(self, params):
        self.params = params.Copy()
        p = self.params
        # Subclasses should populate these fields.
        self.query_nodes = None
        self.workload_info = None
        self.train_nodes = None
        self.test_nodes = None

        if p.loop_through_queries:
            self.queries_permuted = False
            self.queries_ptr = 0

    def _ensure_queries_permuted(self, rng):
        """Permutes queries once."""
        if not self.queries_permuted:
            self.query_nodes = rng.permutation(self.query_nodes)
            self.queries_permuted = True

    def _get_sql_set(self, query_dir, query_glob, is_test = False, skip_neo_processed = False, skip_balsa_processed = False, require_loger_dir = False):
        if query_glob is None:
            return set()

        globs = query_glob if isinstance(query_glob, list) else [query_glob]
        print(f"Searching {query_dir} for SQL files matching patterns: {globs}")
        
        sql_files = np.concatenate([
            glob.glob(f'{query_dir}/**/{pattern}', recursive=True)
            for pattern in tqdm(globs, desc=f"Searching {query_dir} for SQL files")
        ]).ravel()
        
        if not is_test:
            sql_files = set(sql_files)
            return sql_files
        else:
            filtered_files = []
            for sql_file in tqdm(sql_files, desc="Filtering SQL files"):
                parent_dir = os.path.dirname(sql_file)
                # Skip if NEO processed and flag is set
                
                if skip_neo_processed and os.path.isdir(os.path.join(parent_dir, "NEO")):
                    continue

                # Skip if BALSA processed and flag is set
                if skip_balsa_processed and os.path.isdir(os.path.join(parent_dir, "BALSA")):
                    continue

                # Require LOGER dir
                if require_loger_dir and not os.path.isdir(os.path.join(parent_dir, "LOGER")):
                    continue
                
                filtered_files.append(sql_file)
            
            print(f"Found {len(filtered_files)} SQL files in {query_dir} matching patterns {globs} and active filters.")
            return set(filtered_files)

    def Queries(self, split='all'):
        """Returns all queries as balsa.Node objects."""
        assert split in ['all', 'train', 'test'], split
        if split == 'all':
            return self.query_nodes
        elif split == 'train':
            return self.train_nodes
        elif split == 'test':
            return self.test_nodes

    def WithQueries(self, query_nodes):
        """Replaces this Workload's queries with 'query_nodes'."""
        self.query_nodes = query_nodes
        self.workload_info = plans_lib.WorkloadInfo(query_nodes)

    def FilterQueries(self, query_dir, query_glob, test_query_glob):
        all_sql_set_new = self._get_sql_set(query_dir, query_glob)
        test_sql_set_new = self._get_sql_set(query_dir, test_query_glob)
        assert test_sql_set_new.issubset(all_sql_set_new), (test_sql_set_new,
                                                            all_sql_set_new)

        all_sql_set = set([n.info['path'] for n in self.query_nodes])
        assert all_sql_set_new.issubset(all_sql_set), (
            'Missing nodes in init_experience; '
            'To fix: remove data/initial_policy_data.pkl, or see README.')

        query_nodes_new = [
            n for n in self.query_nodes if n.info['path'] in all_sql_set_new
        ]
        train_nodes_new = [
            n for n in query_nodes_new
            if test_query_glob is None or n.info['path'] not in test_sql_set_new
        ]
        test_nodes_new = [
            n for n in query_nodes_new if n.info['path'] in test_sql_set_new
        ]
        assert len(train_nodes_new) > 0

        self.query_nodes = query_nodes_new
        self.train_nodes = train_nodes_new
        self.test_nodes = test_nodes_new

    def UseDialectSql(self, p):
        dialect_sql_dir = p.engine_dialect_query_dir
        for node in self.query_nodes:
            assert 'sql_str' in node.info and 'query_name' in node.info
            path = os.path.join(dialect_sql_dir,
                                node.info['query_name'] + '.sql')
            assert os.path.isfile(path), '{} does not exist'.format(path)
            with open(path, 'r') as f:
                dialect_sql_string = f.read()
            node.info['sql_str'] = dialect_sql_string


class JoinOrderBenchmark(Workload):

    @classmethod
    def Params(cls):
        p = super().Params()
        # Needs to be an absolute path for rllib.
        module_dir = os.path.abspath(os.path.dirname(balsa.__file__) + '/../')
        p.query_dir = os.path.join(module_dir, 'queries/join-order-benchmark')
        return p

    def __init__(self, params):
        super().__init__(params)
        p = params
        self.query_nodes, self.train_nodes, self.test_nodes = self._LoadQueries_alt()
        # self.query_nodes, self.train_nodes, self.test_nodes = self._LoadQueries()
        self.workload_info = plans_lib.WorkloadInfo(self.query_nodes)
        self.workload_info.SetPhysicalOps(p.search_space_join_ops,
                                          p.search_space_scan_ops)

    def _LoadQueries_alt(self):
        """Loads all queries into balsa.Node objects."""
        p = self.params

        # --- Step 1: Collect ALL SQL files from both primary and test directories ---
        all_sql_files_from_primary_dir = self._get_sql_set(p.query_dir, p.query_glob)
        
        test_sql_files_from_test_dir = set()
        if p.test_query_dir is not None:
            # Note: For workload_order_file scenario, the original logic had `p.query_glob` for test_query_dir.
            # This means test_query_dir queries also adhere to the main query_glob.
            # If `p.test_query_glob` is also specified and different, it should be used for test_query_dir here.
            # For simplicity, assuming `p.query_glob` or explicit `p.test_query_glob` (if not None in p)
            test_glob_pattern = p.test_query_glob if p.test_query_glob is not None else p.query_glob
            
            test_sql_files_from_test_dir = self._get_sql_set(
                p.test_query_dir, 
                test_glob_pattern, # Use test_query_glob if specified, else query_glob
                is_test=True, 
                skip_neo_processed=p.skip_neo_processed, 
                skip_balsa_processed=p.skip_balsa_processed, 
                require_loger_dir=p.require_loger_dir
            )
        else:
            # If no separate test_query_dir, test queries are a subset of primary dir.
            # In this case, test_sql_files_from_test_dir should be populated from primary_dir
            # based on p.test_query_glob.
            if p.test_query_glob is not None:
                test_sql_files_from_test_dir = self._get_sql_set(p.query_dir, p.test_query_glob)
                assert test_sql_files_from_test_dir.issubset(all_sql_files_from_primary_dir), \
                    "Test queries (from test_query_glob within primary dir) must be a subset of primary queries."

        # Combine all unique SQL file paths for vocabulary building
        unified_sql_file_paths = sorted(list(all_sql_files_from_primary_dir.union(test_sql_files_from_test_dir)))
        
        # --- Step 2: Parse ALL queries into Node objects for the global vocabulary ---
        # This 'all_nodes_for_vocab' will be the source for self.query_nodes
        all_nodes_for_vocab = []
        for sqlfile in tqdm(unified_sql_file_paths, desc="Parsing ALL SQL to nodes for global vocabulary"):
            try:
                node = ParseSqlToNode(sqlfile)
                all_nodes_for_vocab.append(node)
            except Exception as e:
                print(f"Warning: Failed to parse query {sqlfile}, skipping for global vocabulary. Error: {str(e)}")
        
        # Now, self.query_nodes will correctly represent the union for workload_info
        self.query_nodes = all_nodes_for_vocab


        # --- Step 3: Define train_nodes and test_nodes based on the original split logic ---
        # These lists will contain references to nodes already in self.query_nodes
        train_nodes_list = []
        test_nodes_list = []
        
        # Identify which paths belong to the training set and which to the test set
        # This re-implements the original logic to split based on glob/order_file
        
        # Scenario 1: workload_order_file is specified
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            with open(p.workload_order_file, 'r') as f:
                ordered_filenames = {line.strip() for line in f if line.strip()}
            
            train_paths = set()
            for filename in ordered_filenames:
                full_path = os.path.join(p.query_dir, filename)
                if full_path in all_sql_files_from_primary_dir: # Ensure it's from the primary train source
                    train_paths.add(full_path)
                else:
                    print(f"Warning: Query file {filename} from order file not found in primary query_dir.")
            
            # Remaining queries (from unified_sql_file_paths) are test queries
            test_paths = unified_sql_file_paths - train_paths
            
            for node in self.query_nodes:
                if node.info['path'] in train_paths:
                    train_nodes_list.append(node)
                elif node.info['path'] in test_paths:
                    test_nodes_list.append(node)
                # If a node isn't in either, it means it was in `test_sql_files_from_test_dir` but not in `train_paths`
                # (due to primary_dir filtering in order_file logic). It will be correctly added to test_nodes_list.

        # Scenario 2: Separate test_query_dir is used (p.test_query_dir is not None)
        elif p.test_query_dir is not None:
            # Training nodes are all nodes from the primary query_dir matching query_glob
            train_paths = all_sql_files_from_primary_dir
            test_paths = test_sql_files_from_test_dir
            
            for node in self.query_nodes:
                if node.info['path'] in train_paths:
                    train_nodes_list.append(node)
                elif node.info['path'] in test_paths:
                    test_nodes_list.append(node)

        # Scenario 3: Default behavior (test_query_glob filters within primary query_dir)
        else:
            train_paths = all_sql_files_from_primary_dir - test_sql_files_from_test_dir
            test_paths = test_sql_files_from_test_dir
            
            for node in self.query_nodes:
                if node.info['path'] in train_paths:
                    train_nodes_list.append(node)
                elif node.info['path'] in test_paths:
                    test_nodes_list.append(node)

        self.train_nodes = train_nodes_list
        self.test_nodes = test_nodes_list
        
        assert len(self.train_nodes) > 0, "No training queries loaded."
        print(f"Loaded {len(self.train_nodes)} training queries and {len(self.test_nodes)} test queries for this workload instance.")
        
        # Return self.query_nodes (all nodes), self.train_nodes, self.test_nodes
        # Note: self.query_nodes is already updated.
        return self.query_nodes, self.train_nodes, self.test_nodes

    def _LoadQueries(self):
        """Loads all queries into balsa.Node objects."""
        p = self.params
        all_sql_set = self._get_sql_set(p.query_dir, p.query_glob)
        if p.test_query_dir is not None:
            test_sql_set = self._get_sql_set(p.test_query_dir, p.query_glob, is_test=True, skip_neo_processed=p.skip_neo_processed, skip_balsa_processed=p.skip_balsa_processed, require_loger_dir=p.require_loger_dir)
        else:
            test_sql_set = self._get_sql_set(p.query_dir, p.test_query_glob)
            assert test_sql_set.issubset(all_sql_set)

        # If workload order file is specified, use that order
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            with open(p.workload_order_file, 'r') as f:
                # Read lines and remove whitespace/empty lines
                ordered_files = [line.strip() for line in f if line.strip()]
            
            # Create full paths for ordered files (these will be training queries)
            train_sql_list = []
            for filename in tqdm(ordered_files, desc="Processing ordered queries"):
                full_path = os.path.join(p.query_dir, filename)
                if full_path in all_sql_set:
                    train_sql_list.append(full_path)
                else:
                    print(f"Warning: Query file {filename} from order file not found in query_dir")
            
            # All remaining queries are test queries
            remaining_queries = sorted(all_sql_set - set(train_sql_list))
            print(f"Adding {len(remaining_queries)} remaining queries as test nodes")
            
            # Combine train and test queries (train first, then test)
            all_sql_list = train_sql_list + remaining_queries
        else:
            # Default behavior: sorted by query id for easy debugging
            all_sql_list = sorted(all_sql_set)

        all_nodes = [ParseSqlToNode(sqlfile) for sqlfile in tqdm(all_sql_list, desc="Parsing SQL to nodes")]

        # In ordered mode, train nodes are exactly those from the order file
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            train_nodes = [n for n in all_nodes if n.info['path'] in set(train_sql_list)]
            test_nodes = [n for n in all_nodes if n.info['path'] in set(remaining_queries)]
        elif p.test_query_dir is not None:
            train_nodes = [n for n in all_nodes if n.info['path'] in set(all_sql_list)]
            test_sql_list = sorted(test_sql_set)
            test_nodes = []
            for sqlfile in tqdm(test_sql_list, desc="Parsing test SQL nodes"):
                try:
                    node = ParseSqlToNode(sqlfile)
                    test_nodes.append(node)
                except Exception as e:
                    print(f"Warning: Failed to parse test query {sqlfile}, skipping. Error: {str(e)}")
            # test_nodes = [ParseSqlToNode(sqlfile) for sqlfile in tqdm(test_sql_list, desc="Parsing test SQL nodes")]
        else:
            # Default behavior
            train_nodes = [
                n for n in all_nodes
                if p.test_query_glob is None or n.info['path'] not in test_sql_set
            ]
            test_nodes = [n for n in all_nodes if n.info['path'] in test_sql_set]
        
        assert len(train_nodes) > 0
        print(f"Loaded {len(train_nodes)} training queries and {len(test_nodes)} test queries")
        return all_nodes, train_nodes, test_nodes

# Manually added for the STACK database/workload
class STACK(Workload):

    @classmethod
    def Params(cls):
        p = super().Params()
        # Needs to be an absolute path for rllib.
        module_dir = os.path.abspath(os.path.dirname(balsa.__file__) + '/../')
        p.query_dir = os.path.join(module_dir, 'queries/stack')
        return p

    def __init__(self, params):
        super().__init__(params)
        p = params
        self.query_nodes, self.train_nodes, self.test_nodes = \
            self._LoadQueries()
        self.workload_info = plans_lib.WorkloadInfo(self.query_nodes)
        self.workload_info.SetPhysicalOps(p.search_space_join_ops,
                                          p.search_space_scan_ops)

    def _LoadQueries(self):
        """Loads all queries into balsa.Node objects."""
        p = self.params
        all_sql_set = self._get_sql_set(p.query_dir, p.query_glob)
        test_sql_set = self._get_sql_set(p.query_dir, p.test_query_glob)
        assert test_sql_set.issubset(all_sql_set)

        # If workload order file is specified, use that order
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            with open(p.workload_order_file, 'r') as f:
                # Read lines and remove whitespace/empty lines
                ordered_files = [line.strip() for line in f if line.strip()]
            
            # Create full paths for ordered files (these will be training queries)
            train_sql_list = []
            for filename in ordered_files:
                full_path = os.path.join(p.query_dir, filename)
                if full_path in all_sql_set:
                    train_sql_list.append(full_path)
                else:
                    print(f"Warning: Query file {filename} from order file not found in query_dir")
            
            # All remaining queries are test queries
            remaining_queries = sorted(all_sql_set - set(train_sql_list))
            print(f"Adding {len(remaining_queries)} remaining queries as test nodes")
            
            # Combine train and test queries (train first, then test)
            all_sql_list = train_sql_list + remaining_queries
        else:
            # Default behavior: sorted by query id for easy debugging
            all_sql_list = sorted(all_sql_set)

        all_nodes = [ParseSqlToNode(sqlfile) for sqlfile in all_sql_list]

        # In ordered mode, train nodes are exactly those from the order file
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            train_nodes = [n for n in all_nodes if n.info['path'] in set(train_sql_list)]
            test_nodes = [n for n in all_nodes if n.info['path'] in set(remaining_queries)]
        else:
            # Default behavior
            train_nodes = [
                n for n in all_nodes
                if p.test_query_glob is None or n.info['path'] not in test_sql_set
            ]
            test_nodes = [n for n in all_nodes if n.info['path'] in test_sql_set]
        
        assert len(train_nodes) > 0
        print(f"Loaded {len(train_nodes)} training queries and {len(test_nodes)} test queries")

        return all_nodes, train_nodes, test_nodes

# Manually added for the TPC-H database/workload
class TPCH(Workload):

    @classmethod
    def Params(cls):
        p = super().Params()
        # Needs to be an absolute path for rllib.
        module_dir = os.path.abspath(os.path.dirname(balsa.__file__) + '/../')
        p.query_dir = os.path.join(module_dir, 'queries/tpch')
        return p

    def __init__(self, params):
        super().__init__(params)
        p = params
        self.query_nodes, self.train_nodes, self.test_nodes = \
            self._LoadQueries()
        self.workload_info = plans_lib.WorkloadInfo(self.query_nodes)
        self.workload_info.SetPhysicalOps(p.search_space_join_ops,
                                          p.search_space_scan_ops)

    def _LoadQueries(self):
        """Loads all queries into balsa.Node objects."""
        p = self.params
        all_sql_set = self._get_sql_set(p.query_dir, p.query_glob)
        test_sql_set = self._get_sql_set(p.query_dir, p.test_query_glob)
        assert test_sql_set.issubset(all_sql_set)

        # If workload order file is specified, use that order
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            with open(p.workload_order_file, 'r') as f:
                # Read lines and remove whitespace/empty lines
                ordered_files = [line.strip() for line in f if line.strip()]
            
            # Create full paths for ordered files (these will be training queries)
            train_sql_list = []
            for filename in ordered_files:
                full_path = os.path.join(p.query_dir, filename)
                if full_path in all_sql_set:
                    train_sql_list.append(full_path)
                else:
                    print(f"Warning: Query file {filename} from order file not found in query_dir")
            
            # All remaining queries are test queries
            remaining_queries = sorted(all_sql_set - set(train_sql_list))
            print(f"Adding {len(remaining_queries)} remaining queries as test nodes")
            
            # Combine train and test queries (train first, then test)
            all_sql_list = train_sql_list + remaining_queries
        else:
            # Default behavior: sorted by query id for easy debugging
            all_sql_list = sorted(all_sql_set)

        print(f"Loading {len(all_sql_set)} queries from {p.query_dir} with glob {p.query_glob}")
        all_nodes = [ParseSqlToNode(sqlfile) for sqlfile in all_sql_list]

        # In ordered mode, train nodes are exactly those from the order file
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            train_nodes = [n for n in all_nodes if n.info['path'] in set(train_sql_list)]
            test_nodes = [n for n in all_nodes if n.info['path'] in set(remaining_queries)]
        else:
            # Default behavior
            train_nodes = [
                n for n in all_nodes
                if p.test_query_glob is None or n.info['path'] not in test_sql_set
            ]
            test_nodes = [n for n in all_nodes if n.info['path'] in test_sql_set]
        
        assert len(train_nodes) > 0
        print(f"Loaded {len(train_nodes)} training queries and {len(test_nodes)} test queries")

        return all_nodes, train_nodes, test_nodes

# Manually added for the TPC-DS database/workload
class TPCDS(Workload):

    @classmethod
    def Params(cls):
        p = super().Params()
        # Needs to be an absolute path for rllib.
        module_dir = os.path.abspath(os.path.dirname(balsa.__file__) + '/../')
        p.query_dir = os.path.join(module_dir, 'queries/tpcds')
        return p

    def __init__(self, params):
        super().__init__(params)
        p = params
        self.query_nodes, self.train_nodes, self.test_nodes = \
            self._LoadQueries()
        self.workload_info = plans_lib.WorkloadInfo(self.query_nodes)
        self.workload_info.SetPhysicalOps(p.search_space_join_ops,
                                          p.search_space_scan_ops)

    def _LoadQueries(self):
        """Loads all queries into balsa.Node objects."""
        p = self.params
        all_sql_set = self._get_sql_set(p.query_dir, p.query_glob)
        test_sql_set = self._get_sql_set(p.query_dir, p.test_query_glob)
        assert test_sql_set.issubset(all_sql_set)

        # If workload order file is specified, use that order
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            with open(p.workload_order_file, 'r') as f:
                # Read lines and remove whitespace/empty lines
                ordered_files = [line.strip() for line in f if line.strip()]
            
            # Create full paths for ordered files (these will be training queries)
            train_sql_list = []
            for filename in ordered_files:
                full_path = os.path.join(p.query_dir, filename)
                if full_path in all_sql_set:
                    train_sql_list.append(full_path)
                else:
                    print(f"Warning: Query file {filename} from order file not found in query_dir")
            
            # All remaining queries are test queries
            remaining_queries = sorted(all_sql_set - set(train_sql_list))
            print(f"Adding {len(remaining_queries)} remaining queries as test nodes")
            
            # Combine train and test queries (train first, then test)
            all_sql_list = train_sql_list + remaining_queries
        else:
            # Default behavior: sorted by query id for easy debugging
            all_sql_list = sorted(all_sql_set)

        all_nodes = [ParseSqlToNode(sqlfile) for sqlfile in all_sql_list]

        # In ordered mode, train nodes are exactly those from the order file
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            train_nodes = [n for n in all_nodes if n.info['path'] in set(train_sql_list)]
            test_nodes = [n for n in all_nodes if n.info['path'] in set(remaining_queries)]
        else:
            # Default behavior
            train_nodes = [
                n for n in all_nodes
                if p.test_query_glob is None or n.info['path'] not in test_sql_set
            ]
            test_nodes = [n for n in all_nodes if n.info['path'] in test_sql_set]
        
        assert len(train_nodes) > 0
        print(f"Loaded {len(train_nodes)} training queries and {len(test_nodes)} test queries")

        return all_nodes, train_nodes, test_nodes

# Manually added for the SSB database/workload
class SSB(Workload):

    @classmethod
    def Params(cls):
        p = super().Params()
        # Needs to be an absolute path for rllib.
        module_dir = os.path.abspath(os.path.dirname(balsa.__file__) + '/../')
        p.query_dir = os.path.join(module_dir, 'queries/ssb')
        return p

    def __init__(self, params):
        super().__init__(params)
        p = params
        self.query_nodes, self.train_nodes, self.test_nodes = \
            self._LoadQueries()
        self.workload_info = plans_lib.WorkloadInfo(self.query_nodes)
        self.workload_info.SetPhysicalOps(p.search_space_join_ops,
                                          p.search_space_scan_ops)

    def _LoadQueries(self):
        """Loads all queries into balsa.Node objects."""
        p = self.params
        all_sql_set = self._get_sql_set(p.query_dir, p.query_glob)
        test_sql_set = self._get_sql_set(p.query_dir, p.test_query_glob)
        assert test_sql_set.issubset(all_sql_set)

        # If workload order file is specified, use that order
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            with open(p.workload_order_file, 'r') as f:
                # Read lines and remove whitespace/empty lines
                ordered_files = [line.strip() for line in f if line.strip()]
            
            # Create full paths for ordered files (these will be training queries)
            train_sql_list = []
            for filename in ordered_files:
                full_path = os.path.join(p.query_dir, filename)
                if full_path in all_sql_set:
                    train_sql_list.append(full_path)
                else:
                    print(f"Warning: Query file {filename} from order file not found in query_dir")
            
            # All remaining queries are test queries
            remaining_queries = sorted(all_sql_set - set(train_sql_list))
            print(f"Adding {len(remaining_queries)} remaining queries as test nodes")
            
            # Combine train and test queries (train first, then test)
            all_sql_list = train_sql_list + remaining_queries
        else:
            # Default behavior: sorted by query id for easy debugging
            all_sql_list = sorted(all_sql_set)

        all_nodes = [ParseSqlToNode(sqlfile) for sqlfile in all_sql_list]

        # In ordered mode, train nodes are exactly those from the order file
        if p.workload_order_file and os.path.isfile(p.workload_order_file):
            train_nodes = [n for n in all_nodes if n.info['path'] in set(train_sql_list)]
            test_nodes = [n for n in all_nodes if n.info['path'] in set(remaining_queries)]
        else:
            # Default behavior
            train_nodes = [
                n for n in all_nodes
                if p.test_query_glob is None or n.info['path'] not in test_sql_set
            ]
            test_nodes = [n for n in all_nodes if n.info['path'] in test_sql_set]
        
        assert len(train_nodes) > 0
        print(f"Loaded {len(train_nodes)} training queries and {len(test_nodes)} test queries")

        return all_nodes, train_nodes, test_nodes

class RunningStats(object):
    """Computes running mean and standard deviation.

    Usage:
        rs = RunningStats()
        for i in range(10):
            rs.Record(np.random.randn())
        print(rs.Mean(), rs.Std())
    """

    def __init__(self, n=0., m=None, s=None):
        self.n = n
        self.m = m
        self.s = s

    def Record(self, x):
        self.n += 1
        if self.n == 1:
            self.m = x
            self.s = 0.
        else:
            prev_m = self.m.copy()
            self.m += (x - self.m) / self.n
            self.s += (x - prev_m) * (x - self.m)

    def Mean(self):
        return self.m if self.n else 0.0

    def Variance(self):
        return self.s / (self.n) if self.n else 0.0

    def Std(self, epsilon_guard=True):
        eps = 1e-6
        std = np.sqrt(self.Variance())
        if epsilon_guard:
            return np.maximum(eps, std)
        return std
