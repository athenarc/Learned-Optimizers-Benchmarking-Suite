import os
import json
import pickle
import torch
import numpy as np
import random
from pathlib import Path
import logging
import argparse
import yaml
from database_env import *
from algorithm.neo import *
from model import *
from sql_parser.generate_query_json import *
from sql_parser.config import *

# Configuration
MODEL_DIR = "/data/hdd1/users/kmparmp/models/neo/job"
CHECKPOINT_PATH = os.path.join(MODEL_DIR, "checkpoint_ep11300.pt")  # Last checkpoint
RESULTS_DIR = "/data/hdd1/users/kmparmp/models/neo/test_results"
SEED = 123

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import numpy as np

def convert_numpy_types(obj):
    if isinstance(obj, np.generic):
        return obj.item()  # Convert numpy types to native Python types
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_numpy_types(x) for x in obj]
    return obj

class NeoEvaluator:
    def __init__(self, neo: Neo, env_config: Dict, results_root: str):
        """
        Initialize the Neo evaluator with enhanced file path handling.
        
        Args:
            neo: Trained Neo model instance
            env_config: Database environment configuration containing query file paths
            results_root: Root directory for storing all evaluation results
        """
        self.neo = neo
        self.env_config = env_config
        self.results_root = results_root
        os.makedirs(self.results_root, exist_ok=True)
        
        # Create database environment
        self.env = DataBaseEnv(env_config)
        
        # Build query_name to file_path mapping
        self.query_paths = self._build_query_path_mapping()
        
    def _build_query_path_mapping(self) -> Dict[str, str]:
        """Extract file paths from the environment configuration"""
        query_paths = {}
        for query_name, query_data in self.env_config['db_data'].items():
            # The file path is stored as the last element in query_data
            if len(query_data) >= 5:  # Ensure the structure matches generate_test_query_json
                file_path = query_data[4]
                query_paths[query_name] = file_path
            else:
                logger.warning(f"Query {query_name} is missing file path in env config")
        return query_paths
    
    def evaluate_test_queries(self) -> Dict:
        """
        Evaluate all test queries specified in the environment config.
        Automatically retrieves file paths from the environment configuration.
        """
        test_queries = self.env_config.get('test_queries', [])
        
        if not test_queries:
            logger.error("No test queries found in environment config")
            return {}
            
        # Filter out queries that don't have file paths
        valid_queries = [q for q in test_queries if q in self.query_paths]
        missing_queries = set(test_queries) - set(valid_queries)
        
        if missing_queries:
            logger.warning(f"{len(missing_queries)} test queries missing file paths")
            
        return self.evaluate_queries(valid_queries)
    
    def evaluate_queries(self, query_names: List[str]) -> Dict:
        """
        Evaluate specific queries, fetching their file paths from the environment config.
        """
        results = {
            'queries': [],
            'latencies': [],
            'plans': [],
            'metrics': [],
            'query_paths': []
        }
        
        from tqdm import tqdm
        for query_name in tqdm(query_names, desc="Evaluating queries"):
            file_path = self.query_paths.get(query_name)
            print(f"Evaluating query {query_name} from {file_path}")
            if not file_path:
                logger.warning(f"Skipping {query_name} - no file path available")
                continue
                
            try:
                query_result = self._evaluate_single_query(query_name, file_path)
                results['queries'].append(query_name)
                results['latencies'].append(query_result['actual_latency'])
                results['plans'].append(query_result['plan_path'])
                results['metrics'].append(query_result['metrics'])
                results['query_paths'].append(file_path)
                
                logger.info(f"Evaluated {query_name}: "
                          f"Latency={query_result['metrics']['actual_latency']:.2f}ms, "
                          f"Q-error={query_result['metrics']['q_error']:.2f}")
                
            except Exception as e:
                print(f"Error evaluating query {query_name}: {str(e)}")
                logger.error(f"Error evaluating query {query_name}: {str(e)}", exc_info=True)
                continue
        
        return results
    
    def _evaluate_single_query(self, query_name: str, file_path: str) -> Dict:
        """
        Evaluate a single query and save results in its NEO subdirectory.
        
        Args:
            query_name: Name of the query to evaluate
            file_path: Original path of the query file
            
        Returns:
            Dictionary containing evaluation results for this query
        """
        # Create NEO subdirectory
        original_dir = os.path.dirname(file_path)
        neo_dir = os.path.join(original_dir, 'NEO')
        print(f"Creating directory {neo_dir} for query {query_name}")
        os.makedirs(neo_dir, exist_ok=True)
        
        # Create a fresh environment for this evaluation
        print(f"Creating a new environment for query {query_name}")
        eval_env = self.env.__class__(self.env_config)
        eval_env.reset(query_name)
        
        # Generate plan and measure time
        print(f"Generating plan for query {query_name}")
        start_time = time.time()
        plan,predicted_latency = self.neo.generate_plan_with_prediction(query_name)
        inference_time = time.time() - start_time
        
        # Set the generated plan in our evaluation environment
        eval_env.plan = plan
        
        # Evaluate plan (get actual latency)
        print(f"Evaluating plan for query {query_name}")
        predicted_latency = predicted_latency/1000  # Convert to milliseconds
        query_plan,actual_latency = eval_env.explain_analyze()
        actual_latency = actual_latency/1000  # Convert to milliseconds
        # Calculate metrics
        print(f"Calculating metrics for query {query_name}")
        metrics = self._calculate_metrics(predicted_latency, actual_latency, inference_time)
        print(f"Metrics for query {query_name}: {metrics}")
        # Save plan and metrics
        print(f"Saving results for query {query_name}")
        # Save query plan
        qep_path =  os.path.join(neo_dir, f"{query_name}_neo_plan.json")
        # Write the QEP to a file
        with open(qep_path, 'w') as f:
            json.dump(query_plan, f, indent=2)
        plan_path = os.path.join(neo_dir, f"{query_name}_neo_abstract_plan.json")
        metrics_path = os.path.join(neo_dir, f"{query_name}_neo_metrics.json")
        print(f"Saving plan to {plan_path}")
        print(f"Saving metrics to {metrics_path}")
        
        plan.save(plan_path)
        self._save_metrics(metrics, metrics_path)
        
        return {
            'query_name': query_name,
            'plan_path': plan_path,
            'metrics': metrics,
            'actual_latency': actual_latency,
            'predicted_latency': predicted_latency
        }
    
    def _calculate_metrics(self, predicted: float, actual: float, inference_time: float) -> Dict:
        """Calculate evaluation metrics"""
        if actual > 0:
            q_error = max(predicted/actual, actual/predicted)
        else:
            q_error = float('inf')
            
        return {
            'predicted_latency': predicted,
            'actual_latency': actual,
            'q_error': q_error,
            'inference_time': inference_time,
            'timestamp': time.time()
        }

    # Then modify your _save_metrics method:
    def _save_metrics(self, metrics, metrics_path):
        with open(metrics_path, 'w') as f:
            json.dump(convert_numpy_types(metrics), f, indent=2)
    
    def _save_aggregated_results(self, results: Dict):
        """Save aggregated results and summary statistics"""
        # Save detailed results
        detailed_path = os.path.join(self.results_root, "detailed_results.json")
        with open(detailed_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Calculate and save summary statistics
        summary = self._generate_summary(results)
        summary_path = os.path.join(self.results_root, "summary.json")
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"\nEvaluation Summary:")
        logger.info(f"- Queries evaluated: {summary['num_queries']}")
        logger.info(f"- Successful evaluations: {summary['successful_queries']}")
        if 'latency_stats' in summary:
            logger.info(f"- Average latency: {summary['latency_stats']['avg']:.2f}ms")
            logger.info(f"- Average Q-error: {summary['q_error_stats']['avg']:.2f}")
    
    def _generate_summary(self, results: Dict) -> Dict:
        """Generate summary statistics from evaluation results"""
        if not results['latencies']:
            return {'error': 'No queries were successfully evaluated'}
        
        latencies = results['latencies']
        q_errors = [m['q_error'] for m in results['metrics'] if m['q_error'] != float('inf')]
        inference_times = [m['inference_time'] for m in results['metrics']]
        
        return {
            'num_queries': len(results['query_paths']),
            'successful_queries': len(results['queries']),
            'failed_queries': len(results['query_paths']) - len(results['queries']),
            'latency_stats': {
                'avg': np.mean(latencies),
                'total': np.sum(latencies),
                'min': np.min(latencies),
                'max': np.max(latencies),
                'std': np.std(latencies),
                'unit': 'ms'
            },
            'q_error_stats': {
                'avg': np.mean(q_errors) if q_errors else float('nan'),
                'min': np.min(q_errors) if q_errors else float('nan'),
                'max': np.max(q_errors) if q_errors else float('nan'),
                'std': np.std(q_errors) if q_errors else float('nan'),
                'median': np.median(q_errors) if q_errors else float('nan')
            },
            'inference_stats': {
                'avg': np.mean(inference_times),
                'total': np.sum(inference_times),
                'max': np.max(inference_times),
                'unit': 'seconds'
            },
            'timestamp': time.time()
        }



def load_configs(config_path, env_config_path):
    """Load configuration files"""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    with open(env_config_path, 'r') as f:
        env_config = json.load(f)
    return config, env_config

def load_model(checkpoint_path, config, env_config, device='cuda'):
    """Load the trained Neo model from checkpoint"""
    model = NeoTreeConvNet(**config['net_args'])
    agent = Agent(model, collate_fn=collate, device=device)
    
    checkpoint = torch.load(checkpoint_path, map_location=device)
    agent.net.load_state_dict(checkpoint['model_state_dict'])
    agent.net.to(device)
    agent.net.eval()
    agent.eps = 0  # Disable exploration during testing
    
    # Get neo_args from config
    neo_args = config['neo_args']
    
    # Update device setting
    neo_args['device'] = device
    
    # Create a Neo instance with the trained agent
    neo = Neo(
        agent=agent,
        env_config=env_config,
        args=neo_args,
        train_args=config['train_args'],
        baseline_plans={}  # You might want to load baseline plans if available
    )
    
    return neo

def prepare_test_environment(env_config, test_queries):
    """Prepare the database environment for testing"""
    test_env_config = env_config.copy()
    # Remove test queries from training data
    test_env_config['db_data'] = {k: v for k, v in test_env_config['db_data'].items() 
                                 if k not in test_queries}
    # Enable latency measurement
    test_env_config['return_latency'] = True
    return DataBaseEnv(test_env_config)

def evaluate_model(neo, env, test_queries, results_dir):
    """Evaluate the model on test queries and save results in structured directories"""
    results = {
        'queries': [],
        'latencies': [],
        'plans': [],
        'rewards': [],
        'metrics': []
    }
    
    os.makedirs(results_dir, exist_ok=True)
    
    for query in test_queries:
        try:
            # Generate plan using the trained neo model
            plan = neo.generate_plan(query)
            
            # Evaluate the plan
            env.plan = plan
            latency = env.reward()
            
            # Store results
            results['queries'].append(query)
            results['latencies'].append(latency)
            results['plans'].append(plan)
            results['rewards'].append(-latency)  # Assuming reward is negative latency
            
        except Exception as e:
            logger.error(f"Error evaluating query {query}: {str(e)}")
            continue
    
    # Save detailed results
    results_file = os.path.join(results_dir, "detailed_results.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Calculate and save summary statistics
    if results['latencies']:
        avg_latency = np.mean(results['latencies'])
        total_latency = np.sum(results['latencies'])
        summary = {
            'num_queries': len(test_queries),
            'successful_queries': len(results['queries']),
            'failed_queries': len(test_queries) - len(results['queries']),
            'avg_latency': avg_latency,
            'total_latency': total_latency,
            'min_latency': np.min(results['latencies']),
            'max_latency': np.max(results['latencies'])
        }
    else:
        summary = {'error': 'No queries were successfully evaluated'}
    
    summary_file = os.path.join(results_dir, "summary.json")
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"\nTest Summary:")
    logger.info(f"- Queries attempted: {len(test_queries)}")
    logger.info(f"- Successful evaluations: {len(results['queries'])}")
    if results['latencies']:
        logger.info(f"- Average latency: {avg_latency:.4f} seconds")
        logger.info(f"- Total latency: {total_latency:.2f} seconds")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Test Neo model")
    parser.add_argument("queries_dir", help="Path to queries directory")
    parser.add_argument("config_path", help="Path to model config file")
    parser.add_argument("--device", default="cuda", help="Device to use (cuda/cpu)")
    args = parser.parse_args()

    conn = psycopg2.connect(DB_URL)
    env_config = generate_complete_test_json(args.queries_dir, conn)

    # Write the json to the config file
    with open("config/pg_job_test_config.json", "w") as f:
        json.dump(env_config, f, indent=4)    

    with open(args.config_path, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    # Set random seeds
    torch.manual_seed(SEED)
    np.random.seed(SEED)
    random.seed(SEED)
    neo = load_model(CHECKPOINT_PATH, config, env_config, args.device)

    # Initialize evaluator
    evaluator = NeoEvaluator(
        neo=neo,
        env_config=env_config,
        results_root=args.queries_dir
    )
    
    test_queries = env_config.get('test_queries', [])
    
    if not test_queries:
        logger.error("No test queries found in environment config")
        return
    
    logger.info(f"Starting evaluation on {len(test_queries)} test queries")
    results = evaluator.evaluate_test_queries()
    
    logger.info(f"\nEvaluation complete.")

if __name__ == "__main__":
    from datetime import datetime
    main()