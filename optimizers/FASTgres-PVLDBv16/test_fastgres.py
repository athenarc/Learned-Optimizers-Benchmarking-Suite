import os
import json
import pickle
import time
from datetime import datetime
import argparse
import numpy as np
from tqdm import tqdm
from copy import deepcopy
import shutil
import argparse
import itertools
import os.path
import time
import numpy as np
import featurize
import utility as u
from copy import deepcopy
from alive_progress import alive_it
from sklearn.ensemble import GradientBoostingClassifier
from hint_sets import HintSet
from featurize import encode_query
from query import Query
from context_heuristic import merge_context_queries
from utility import clear_cache, evaluate_and_store_qep
import os
import shutil
import json
import pickle
from datetime import datetime
from pathlib import Path

class QueryObserver:

    def __init__(self, seed: int, context: frozenset, archive: dict, experience: dict,
                 absolute_time_gate: float, relative_time_gate: int, query_path: str, db_string: str,
                 estimators: int = 100, depth: int = 1000):
        # the model part
        self.seed = seed
        self.estimators = estimators
        self.depth = depth
        self.archive = archive
        self.context = context
        # query_name -> featurization | label | time
        self.experience = experience
        self.model = None
        # the timeout part
        self.absolute = absolute_time_gate
        self.relative = relative_time_gate

        self.timeout = None
        self.update_timeout()
        self.critical_queries = set()

        self.new_model = None
        self.cooldown = 0
        self.critical = u.tree()
        self.path = query_path
        self.db_string = db_string

    def __str__(self) -> str:
        return "Gradient Boosting Observer (est: {}, d: {}) on Context: {} using {} Experiences and Timeout: {}" \
            .format(self.estimators, self.depth, self.context, len(self.experience.keys()), self.timeout)

    def update_timeout(self) -> None:
        old_timeout = self.timeout  # for debug info
        experienced_time = list()
        for query_name in self.experience:
            experienced_time.append(self.experience[query_name]["time"])
        new_timeout = np.percentile(experienced_time, self.relative)
        self.timeout = max(self.absolute, new_timeout)
        print("Updated context: {} timeout: {} -> {}".format(self.context, old_timeout, new_timeout))
        return

    def train(self) -> None:
        new_model = GradientBoostingClassifier(n_estimators=self.estimators,
                                               max_depth=self.depth,
                                               random_state=self.seed)
        x_values = [self.experience[query_name]["featurization"] for query_name in self.experience]
        y_values = [self.experience[query_name]["label"] for query_name in self.experience]
        new_model = new_model.fit(x_values, y_values)
        # new models are not deployed right away since they might be subject of cooldown restrictions
        # unless no initial model is present
        if self.model is None:
            self.model = new_model
        else:
            self.new_model = new_model
        return

    def predict(self, query_featurization: list[float]) -> int:
        # we look up our experience for convenient overlaps
        # -> this should be especially useful for learning to represent
        # can be commented out for true predictions for every query

        # for query_name in self.experience:
        #     exp_featurization = self.experience[query_name]["featurization"]
        #     if np.array_equal(exp_featurization, query_featurization):
        #         return int(self.experience[query_name]["label"])
        
        prediction = self.model.predict(np.reshape(query_featurization, (1, -1)))
        print("Predicted: {}".format(prediction))
        return int(self.model.predict(np.reshape(query_featurization, (1, -1)))[0])

    def move_critical_to_experience(self) -> float:
        labeling_time = 0.0
        for query_name in self.critical:
            self.experience[query_name]["featurization"] = deepcopy(self.critical[query_name]["featurization"])
            self.experience[query_name]["label"] = self.critical[query_name]["label"]
            self.experience[query_name]["time"] = self.critical[query_name]["time"]
            labeling_time += self.critical[query_name]["time"]
            # no influence on the performance, just for capturing info
            self.critical_queries.add(query_name)
        # all critical queries have been taken over to experience
        self.critical = u.tree()
        return labeling_time

    def run_observed_query(self, query_name: str, query_featurization: list[float], context_models) -> int:

        # first, check if we can deploy a new model for prediction
        if self.new_model is not None and self.cooldown <= 0:
            print("Deploying new model on context: {}".format(self.context))
            self.model = self.new_model
            self.new_model = None
            self.cooldown = 0

        # Keep track of the inference time for the prediction
        t0 = time.time()
        prediction = self.predict(query_featurization)
        inference_time = time.time() - t0
        hint_set = HintSet(prediction)

        # second, check archive to speed up the simulated scenario
        try:
            result_time = self.archive[query_name][str(prediction)]
        except KeyError:
            print("Defaulting to pg evaluation, hint set: {} should be caught".format(prediction))
            # no archive info found -> manual eval in server | for client side eval we need all predictions
            result_time = u.evaluate_hinted_query(self.path, query_name, hint_set, self.db_string, self.timeout)
            # if learned path is given, this segment will later be saved
            self.archive[query_name][str(prediction)] = result_time

        # 2 scenarios: time, None may be possible here, archive time might be worse than the timeout!
        if result_time is None or result_time >= self.timeout:
            # the query is critical and should and possibly trigger retraining
            # same format as experience for easy handling
            print("Caught timeout")
            # at this point we already decide to return PG default as retraining does not influence this decision
            result_time = self.archive[query_name]["63"]

            if self.cooldown <= 0 and self.new_model is None:
                self.critical[query_name]["featurization"] = deepcopy(query_featurization)
                self.critical[query_name]["label"] = self.archive[query_name]["opt"]
                self.critical[query_name]["time"] = self.archive[query_name][str(self.archive[query_name]["opt"])]

                # we have capacity to train a new model, double check just in case
                # first we need to move any critical query to our experience
                # these queries decide on how long our cooldown will be
                # these four steps should always be taken when retraining
                labeling_time = self.move_critical_to_experience()
                self.train()
                self.update_timeout()
                self.cooldown += labeling_time
            else:
                # we still have a model that is being trained
                # -> we can additionally deduct the timeout for critical queries
                for c in context_models:
                    model = context_models[c]
                    if not isinstance(model, int):
                        model.cooldown -= self.timeout
                # self.cooldown -= self.timeout

        # At the end, we have to deduct our query runtime from the current cooldown
        # self.cooldown -= result_time
        for c in context_models:
            model = context_models[c]
            if not isinstance(model, int):
                model.cooldown -= result_time
        return prediction


# Configuration - Update these paths as needed
MODEL_DIR = "/data/hdd1/users/kmparmp/models/FASTgres/tpch/frozen_evaluations"
FINAL_EVALUATION_DIR = os.path.join(MODEL_DIR, "frozen_20250502_133314")
RESULTS_DIR = "/data/hdd1/users/kmparmp/models/FASTgres/test_results"

def load_frozen_evaluation(frozen_dir):
    """Load a frozen evaluation package with proper class definitions"""
    required_files = [
        "metadata.json",
        "models/",
        "inputs/archive.json",
        "inputs/query_objects.pkl",
        "inputs/train_test_split.json",
        "inputs/db_type_dict.json",
        "inputs/mm_dict.pkl",
        "inputs/label_encoders.pkl",
        "inputs/wildcard_dict.json"
    ]
    
    # Validate directory structure
    for file in required_files:
        if not os.path.exists(os.path.join(frozen_dir, file)):
            raise FileNotFoundError(f"Missing required file: {file}")
    
    try:
        # Load metadata first
        with open(os.path.join(frozen_dir, "metadata.json")) as f:
            metadata = json.load(f)
        
        inputs_dir = os.path.join(frozen_dir, "inputs")
        
        # Load database metadata
        with open(os.path.join(inputs_dir, "db_type_dict.json")) as f:
            d_type_dict = json.load(f)
        
        with open(os.path.join(inputs_dir, "mm_dict.pkl"), 'rb') as f:
            mm_dict = pickle.load(f)
        
        with open(os.path.join(inputs_dir, "label_encoders.pkl"), 'rb') as f:
            enc_dict = pickle.load(f)
        
        with open(os.path.join(inputs_dir, "wildcard_dict.json")) as f:
            wc_dict = json.load(f)
        
        # Load skipped_dict if exists
        skipped_dict = {}
        skipped_path = os.path.join(inputs_dir, "skipped_dict.pkl")
        if os.path.exists(skipped_path):
            with open(skipped_path, 'rb') as f:
                skipped_dict = pickle.load(f)
        
        # Load models
        context_models = {}
        models_dir = os.path.join(frozen_dir, "models")
        for model_file in os.listdir(models_dir):
            if model_file.endswith('.pkl'):
                with open(os.path.join(models_dir, model_file), 'rb') as f:
                    model = pickle.load(f)
                    context_models[model.context] = model
        
        # Load input data
        with open(os.path.join(inputs_dir, "archive.json")) as f:
            archive = json.load(f)
        
        with open(os.path.join(inputs_dir, "query_objects.pkl"), 'rb') as f:
            query_object_dict = pickle.load(f)
        
        with open(os.path.join(inputs_dir, "train_test_split.json")) as f:
            test_queries = json.load(f).get("test_queries", [])
        
        return {
            "context_models": context_models,
            "archive": archive,
            "query_object_dict": query_object_dict,
            "test_queries": test_queries,
            "metadata": {
                "original": metadata,
                "query_path": metadata["config"]["query_path"],
                "mm_dict": mm_dict,
                "enc_dict": enc_dict,
                "wc_dict": wc_dict,
                "d_type_dict": d_type_dict,
                "skipped_dict": skipped_dict
            }
        }
    except Exception as e:
        raise RuntimeError(f"Error loading frozen evaluation: {str(e)}")

def test_query(query_name, context_models, query_object_dict, archive, db_string, args_save_path, frozen_data):
    """Test a single query with the loaded models"""
    try:
        query_obj = query_object_dict[query_name]
        context = query_obj.context
        
        # Find the appropriate model for this context
        observer = None
        for ctx in context_models:
            if context in ctx or context == ctx:
                observer = context_models[ctx]
                break
        
        if observer is None:
            print(f"No model found for context {context}, using default plan")
            return None, None, None
        
        start_time = time.time()
        
        if isinstance(observer, int):
            prediction = observer
        else:
            # Featurize the query using loaded dictionaries
            feature_dict = featurize.build_feature_dict(
                query_obj, db_string, 
                frozen_data["metadata"]["mm_dict"], 
                frozen_data["metadata"]["enc_dict"],
                frozen_data["metadata"]["wc_dict"],
                set(), set(), frozen_data["metadata"]["skipped_dict"]
            )
            encoded_query = featurize.encode_query(
                context, 
                feature_dict, 
                frozen_data["metadata"]["d_type_dict"]
            )
            
            # Get prediction
            prediction = observer.predict(encoded_query)
        
        forward_time = time.time() - start_time
        
        # Evaluate latency
        latency = u.evaluate_hinted_query(
            frozen_data["metadata"]["query_path"], 
            query_name, 
            HintSet(prediction), 
            db_string, 
            None
        )
        
        # Log performance
        log_entry = {
            "query": query_name,
            "prediction": prediction,
            "forward_time": forward_time,
            "latency": latency,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(os.path.join(args_save_path, "performance_log.json"), "a") as f:
            json.dump(log_entry, f)
            f.write("\n")
        
        return prediction, forward_time, latency
    
    except Exception as e:
        print(f"Error testing query {query_name}: {str(e)}")
        return None, None, None

def evaluate_workload(test_queries, evaluation_data, db_string, args_save_path):
    """Evaluate the loaded models on a set of test queries"""
    results = {
        "predictions": {},
        "forward_times": {},
        "latencies": {},
        "failed_queries": []
    }
    
    total_latency = 0
    progress_bar = tqdm(test_queries, desc="Testing queries")
    
    for query_name in progress_bar:
        prediction, forward_time, latency = test_query(
            query_name=query_name,
            context_models=evaluation_data["context_models"],
            query_object_dict=evaluation_data["query_object_dict"],
            archive=evaluation_data["archive"],
            db_string=db_string,
            args_save_path=args_save_path,
            frozen_data=evaluation_data  # Pass the entire evaluation_data dict
        )
        
        if prediction is not None:
            results["predictions"][query_name] = prediction
            results["forward_times"][query_name] = forward_time
            results["latencies"][query_name] = latency
            total_latency += latency if latency else 0
            
            progress_bar.set_postfix({
                "Latency": f"{latency:.2f}s" if latency else "N/A",
                "Total": f"{total_latency:.2f}s"
            })
        else:
            results["failed_queries"].append(query_name)
    
    # Calculate statistics
    successful_queries = len(results["predictions"])
    if successful_queries > 0:
        results["total_latency"] = total_latency
        results["avg_latency"] = total_latency / successful_queries
        results["avg_forward_time"] = np.mean(list(results["forward_times"].values()))
    else:
        results["total_latency"] = 0
        results["avg_latency"] = 0
        results["avg_forward_time"] = 0
    
    # Save results
    with open(os.path.join(args_save_path, "summary_results.json"), "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nEvaluation completed on {len(test_queries)} queries:")
    print(f"- Successful: {successful_queries}")
    print(f"- Failed: {len(results['failed_queries'])}")
    print(f"- Total latency: {results['total_latency']:.2f} seconds")
    print(f"- Average latency: {results['avg_latency']:.2f} seconds")
    print(f"- Average prediction time: {results['avg_forward_time']:.4f} seconds")
    
    return results

def recursively_process_queries(root_dir, frozen_data, db_string, save_base_path):
    """
    Recursively process all queries under root_dir and save their QEPs in FASTgres subdirectories.
    """
    processed = 0
    failed = 0
    context_models = frozen_data["context_models"]
    query_object_dict = frozen_data["query_object_dict"]
    
    queries = []    
    for dirpath, dirnames, filenames in os.walk(root_dir):
        query_files = [f for f in filenames if f.endswith('.sql') or f.endswith('.query')]
        
        if not query_files:
            continue

        queries.extend(query_files)
        for query_file in query_files:
            query_name = query_file
            query_path = os.path.join(dirpath, query_file)

            try: 
                if query_name not in query_object_dict:
                    query_object_dict[query_name] = Query(query_name, dirpath + "/")
            except Exception as e:
                print(f"Error loading query object for {query_name}: {str(e)}")
                continue

            query_obj = query_object_dict[query_name]
            context = query_obj.context
            
            # Find the appropriate model for this context
            observer = None
            for ctx in context_models:
                if context in ctx or context == ctx:
                    observer = context_models[ctx]
                    break
            
            if observer is None:
                print(f"No model found for context {context}, using default plan")
                prediction = 63
                forward_time = 0.0
            else:
                start_time = time.time()
                
                if isinstance(observer, int):
                    prediction = observer
                else:
                    # Featurize the query using loaded dictionaries
                    feature_dict = featurize.build_feature_dict(
                        query_obj, db_string, 
                        frozen_data["metadata"]["mm_dict"], 
                        frozen_data["metadata"]["enc_dict"],
                        frozen_data["metadata"]["wc_dict"],
                        set(), set(), frozen_data["metadata"]["skipped_dict"]
                    )
                    encoded_query = featurize.encode_query(
                        context, 
                        feature_dict, 
                        frozen_data["metadata"]["d_type_dict"]
                    )
                    
                    # Get prediction
                    prediction = observer.predict(encoded_query)
                
                forward_time = time.time() - start_time

            try:
                # Predict + evaluate
                execution_time = evaluate_and_store_qep(
                    path = dirpath,
                    query_name=query_name,
                    connection_string=db_string,
                    hint_set=HintSet(prediction),
                    timeout=None,
                )

                # Make sure FASTgres/ subdir exists in current query's folder
                fastgres_dir = os.path.join(dirpath, "FASTgres")
                os.makedirs(fastgres_dir, exist_ok=True)

                # Save QEP result
                result = {
                    "query": query_name,
                    "prediction": prediction,
                    "forward_time": forward_time,
                    "timestamp": datetime.now().isoformat()
                }

                result_file = os.path.join(fastgres_dir, f"{query_name}_fastgres_metrics.json")
                with open(result_file, "w") as f:
                    json.dump(result, f, indent=2)

                print(f"✅ Processed: {query_path} -> {result_file}")
                processed += 1

            except Exception as e:
                print(f"❌ Failed to process {query_path}: {str(e)}")
                failed += 1

    print(f"\n📊 Finished recursive evaluation:")
    print(f"   - Queries processed: {processed}")
    print(f"   - Queries failed: {failed}")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test FASTgres models")
    parser.add_argument("workload_dir", help="Directory containing test queries")
    parser.add_argument("-o", "--output_dir", default=RESULTS_DIR, 
                       help="Directory to save test results")
    parser.add_argument("-D", "--database", default="imdb", 
                       help="Database name")
    args = parser.parse_args()
    
    # Create output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    args_save_path = os.path.join(args.output_dir, f"test_{timestamp}")
    os.makedirs(args_save_path, exist_ok=True)
    
    try:
        # Load frozen evaluation package
        print(f"Loading frozen evaluation from {FINAL_EVALUATION_DIR}")
        evaluation_data = load_frozen_evaluation(FINAL_EVALUATION_DIR)
        
        # Get test queries
        if os.path.isdir(args.workload_dir):
            test_queries = u.get_queries(args.workload_dir)
        else:
            test_queries = evaluation_data["test_queries"]
        
        # print(f"Loaded {len(test_queries)} test queries")
        # print(f"Loaded {len(evaluation_data['context_models'])} context models")

        args_db = args.database
        if args_db == "imdb":
            args_db = u.PG_IMDB
        if args_db == "stack":
            args_db = u.PG_STACK_OVERFLOW
        elif args_db == "stack-2016":
            args_db = u.PG_STACK_OVERFLOW_REDUCED_16
        elif args_db == "stack-2013":
            args_db = u.PG_STACK_OVERFLOW_REDUCED_13
        elif args_db == "stack-2010":
            args_db = u.PG_STACK_OVERFLOW_REDUCED_10
        elif args_db == "tpch":
            args_db = u.PG_TPC_H
        elif args_db == "tpcds":
            args_db = u.PG_TPC_DS

        if os.path.isdir(args.workload_dir):
            recursively_process_queries(
                root_dir=args.workload_dir,
                frozen_data=evaluation_data,
                db_string=args_db,
                save_base_path=args.workload_dir
            )
     
        # # Print summary
        # print("\nTest Summary:")
        # print(f"- Queries tested: {len(test_queries)}")
        # print(f"- Queries succeeded: {len(results['predictions'])}")
        # print(f"- Queries failed: {len(results['failed_queries'])}")
        # print(f"- Total latency: {results['total_latency']:.2f} seconds")
        # print(f"- Average latency: {results['avg_latency']:.2f} seconds")
        # print(f"- Average forward time: {results['avg_forward_time']:.4f} seconds")
        # print(f"\nResults saved to: {args_save_path}")
    
    except Exception as e:
        print(f"Error during testing: {str(e)}")
        raise

if __name__ == "__main__":
    import utility as u
    import featurize
    from hint_sets import HintSet
    
    # Run main function
    main()