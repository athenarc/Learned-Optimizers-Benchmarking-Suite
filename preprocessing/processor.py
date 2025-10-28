import os
from abc import ABCMeta, abstractmethod
from typing import Any, Optional, Tuple
import json
from typing import Any, Dict, List, Optional, Sequence
import pandas as pd
import pglast
# from preprocessing.dist import categorical_dist_on_predefined_bins, categorical_dist, numerical_dist_on_predefined_bins
from dist import numerical_dist as numerical_dist_ori
from dist import tuple_to_list
from query_template import SQLInfoExtractor
from distribution import MetadataDistribution, sample_from_distribution, sample_rng  
import glob
from config import DB_CONFIG
from scipy.spatial.distance import jensenshannon

# TYPES = ["tables", "predicates", "joins", "aliasname_fullname"]
TYPES = ["tables", "predicates", "joins"]

class Processor(metaclass=ABCMeta):
    def load_from_file(self, input_file: str):
        pass

    @abstractmethod
    def load(self, input_path: str):
        raise NotImplementedError

    @abstractmethod
    def compute_dists(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def config(self):
        raise NotImplementedError

def load_config(
        config_path: str, default: Optional[Any] = None
) -> Tuple[dict, Optional[str]]:
    if not os.path.exists(config_path):
        return default if default is not None else {}, "no such file: " + config_path

    try:
        with open(config_path, "r") as f:
            return json.loads(f.read()), None
    except:
        return default if default is not None else {}, "invalid config. ignoring"

    print("config loaded from: ", config_path)

class QueryProcessor(Processor):
    def __init__(self, workload_dir: str, workload_id: str, config_path: str = "config.json", dump_feature_table: bool = False):
        self.dbname = DB_CONFIG["dbname"]
        self.config_path = f"./{workload_id}-query-config.json"
        self.predefined_bins = None
        self.data: Dict[str, List[Any]] = {}
        self.dists: Dict[str, MetadataDistribution] = {}
        self.workload_dir = workload_dir
        self._create = False

        self._config, err = load_config(
            self.config_path, {"bin_values": {}, "map": {}}
        )
        if err is not None:
            print("WARN  loading config: ", err)

            if "no such file" in err:
                self._create = True

    @property
    def config(self):
        return self._config

    def _slots(self, data_dict: Optional[dict] = None):
        if not data_dict:
            data_dict = self.data

        return [str(k) for k in data_dict]

    def load(self):
        sql_files = glob.glob(os.path.join(self.workload_dir, "*.sql"))
        for sql_file in sql_files:
            with open(sql_file, "r") as f:
                text = f.read()

            queryID = os.path.basename(sql_file).split(".")[0]
            sqls = text.split(";")
            sqls = [s.strip() for s in sqls if s.strip()]

            for s in sqls:
                node = pglast.parse_sql(s)
                extractor = SQLInfoExtractor()
                extractor(node)

                info = extractor.info
                for k in info.keys():
                    if k not in self.data:
                        self.data[k] = []

                    info_k_values = sorted(info[k])
                    info_k_values = tuple_to_list(info_k_values)

                    self.data[k].append(info_k_values)

                    if self._create:
                        if k not in self.config["map"]:
                            self.config["map"][k] = {}

                        info_k_values_str = str(info_k_values)
                        if info_k_values_str not in self.config["map"][k]:
                            self.config["map"][k][info_k_values_str] = []

                        self.config["map"][k][info_k_values_str].append(s)

        self.compute_dists()
            
    def compute_dists(self):
        for t in TYPES:
            d = self._get_dist(t, self.data[t])

            if self._create:
                self._update_config(t, d.bin_values)

            self.dists[t] = d

    def _update_config(self, type, bin_values):
        self.config["bin_values"][type] = bin_values

    def _get_dist(self, type: str, data: Sequence[Any]):
        if self._create:
            return MetadataDistribution(type, data)
        else:
            return MetadataDistribution(
                type, data, slots=self._slots(self.config["bin_values"][type])
            )

    def _sample_data(self, dist: List[float], index: list, size: int):
        result = []

        sampled_infos = sample_from_distribution(dist, index, size)[0]
        for info in sampled_infos:
            selections = self.config["map"][self.type][str(info)]
            result.append(sample_rng.choice(selections))

        return result

import json
import numpy as np
import pandas as pd

class NumpyEncoder(json.JSONEncoder):
    """Special json encoder for numpy types"""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

def dump_tbl(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False, header=False, sep="|")

def dump_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False, header=False)

def dump_json(obj, path):
    with open(path, "w") as f:
        f.write(json.dumps(obj, indent=4, cls=NumpyEncoder))


def dump_text(text: str, path: str):
    with open(path, "w") as f:
        f.write(text)

import numpy as np

def compute_workload_distance(qp1: QueryProcessor, qp2: QueryProcessor) -> float:
    import numpy as np

    distances = []

    print("Computing workload distance...")
    print(qp1.dists.get("joins"))
    # TYPES = ["tables", "predicates", "joins"]
    
    for key in TYPES:
        dist1_obj = qp1.dists.get(key)
        dist2_obj = qp2.dists.get(key)

        if dist1_obj is None or dist2_obj is None:
            continue  # Skip this feature if distribution is missing

        # Use .get() to access the Series
        dist1 = dist1_obj.get()
        dist2 = dist2_obj.get()
        
        # Convert to string (if lists are not needed as complex keys)
        dist1.index = dist1.index.map(lambda x: str(x) if isinstance(x, list) else x)
        dist2.index = dist2.index.map(lambda x: str(x) if isinstance(x, list) else x)

        # Union of all bins
        all_bins = dist1.index.union(dist2.index, sort=True)

        # Build aligned vectors
        p = np.array([dist1.get(k, 0.0) for k in all_bins])
        q = np.array([dist2.get(k, 0.0) for k in all_bins])

        # Normalize to form probability distributions
        p = p / (p.sum() if p.sum() != 0 else 1)
        q = q / (q.sum() if q.sum() != 0 else 1)

        # Compute Jensen-Shannon divergence
        jsd = jensenshannon(p, q)
        distances.append(jsd)

    return float(np.mean(distances)) if distances else 0.0

if __name__ == "__main__":
    # Example usage
    from pathlib import Path
    REPO_ROOT = Path(__file__).resolve()
    while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
        REPO_ROOT = REPO_ROOT.parent    
    workload_dir = REPO_ROOT / "workloads" / "imdb_pg_dataset" / "job"
    workload_id = "job"
    qp1 = QueryProcessor(workload_dir=workload_dir, workload_id=workload_id, config_path="config.json")
    qp1.load()

    workload_dir = REPO_ROOT / "workloads" / "imdb_pg_dataset" / "job_extended"
    workload_id = "job_extended"
    qp2 = QueryProcessor(workload_dir=workload_dir, workload_id=workload_id, config_path="config.json")
    qp2.load()

    distance = compute_workload_distance(qp1, qp2)
    print(f"Workload distance: {distance}")