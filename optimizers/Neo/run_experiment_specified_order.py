import shutil
import random
import sys
import yaml
from database_env import *
from algorithm.neo import *
from model import *
from tqdm import tqdm
import pickle
from sql_parser.generate_query_json import *
from sql_parser.config import *
import json
from pathlib import Path
REPO_ROOT = Path(__file__).resolve()
while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
    REPO_ROOT = REPO_ROOT.parent

SEED = 123

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)
MODEL_DIR = f'{REPO_ROOT}/experiment2/job/train/leave_one_out/models/neo/asc_complexity/checkpoints/'
CHECKPOINT_PATH = os.path.join(MODEL_DIR, "checkpoint_ep2260.pt")  # Last checkpoint

def load_model(checkpoint_path, config, env_config, device='cuda', experience=None, baseline_plans=None):
    """Load the trained Neo model from checkpoint"""
    agent = Agent(NeoTreeConvNet(**config['net_args']), collate_fn=collate,
                        device=config['neo_args']['device'])
    
    checkpoint = torch.load(checkpoint_path, map_location=device)
    agent.net.load_state_dict(checkpoint['model_state_dict'])
    agent.net.to(device)
    
    # Create a Neo instance with the trained agent
    neo = Neo(agent, env_config, config['neo_args'],
                config['train_args'], experience, baseline_plans)

    return neo

import argparse
if __name__ == '__main__':
    mp.set_start_method("spawn")
    np.random.seed(123)
    torch.manual_seed(123)
    conn = psycopg2.connect(DB_URL)
    parser = argparse.ArgumentParser(description="Test Neo model")
    parser.add_argument("queries_dir", help="Path to queries directory")
    parser.add_argument("config_path", help="Path to model config file")
    parser.add_argument("--retrain", action='store_true', help="Retrain the model")
    parser.add_argument("--fileorder", default=None, help="Path to .txt file specifying query order")
    args = parser.parse_args()
    # Use cuda if available
    device = torch.device( "cuda" if torch.cuda.is_available() else "cpu" )
    
    env_config = generate_final_json(args.queries_dir, conn, fileorder=args.fileorder)
    parent_dir = os.path.dirname(args.queries_dir)
    
    
    # Write the json to the config file
    with open("config/pg_job_config.json", "w") as f:
        json.dump(env_config, f, indent=4)

    with open(args.config_path, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    Path(config['neo_args']['logdir']).mkdir(parents=True, exist_ok=True)
    shutil.copy(args.config_path, Path(config['neo_args']['logdir']) / 'config.yaml')
    env_config['return_latency'] = config['neo_args']['latency']

    test_set = env_config['test_queries']
    env_config['db_data'] = {
        k: v for k, v in env_config['db_data'].items()}

    output_dir = Path("runs/job_random_ascending_complexity/postgresql/optimizer")
    # Empty the directory if it exists
    if output_dir.exists():
        shutil.rmtree(output_dir)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    # Save the generated query plans
    build_and_save_optimizer_plans(env_config, output_dir)
    
    experience_file = Path(config['neo_args']['logdir']) / 'saved_experience.pkl'

    # Try to load existing experience if available
    if experience_file.exists():
        print("Loading saved experience from file...")
        with open(experience_file, 'rb') as f:
            experience, baseline_plans = pickle.load(f)
    else:

        # load initial experience
        experience = []
        env = DataBaseEnv(env_config)

        # also optimizer plans
        path_plan = Path(config['neo_args']['baseline_path'])
        baseline_plans = {}
        # Initialize counters
        total_files = len(list(path_plan.glob("*.sql.json")))
        print(f"Total files to process: {total_files}")
        processed_files = 0
        skipped_files = 0

        # Create progress bar with additional info
        with tqdm(
            path_plan.glob("*.sql.json"), 
            total=total_files,
            desc="Processing plans",
            unit="file",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
        ) as pbar:
            for p in pbar:
                query = p.parts[-1][:-5]  # Remove '.json' extension
                
                # Update progress bar description with current file
                pbar.set_postfix_str(f"Current: {query[:15]}...")
                
                if test_set and query in test_set:
                    # Skip test files
                    skipped_files += 1
                    pbar.set_postfix_str(f"Skipped: {skipped_files}")
                    continue
                    
                plan = Plan()
                plan.load(p)
                env.plan = plan
                cost = env.reward()
                
                experience.append([env.plan, cost, query])
                baseline_plans[query] = env.plan
                
                processed_files += 1
                pbar.set_postfix({
                    'processed': processed_files,
                    'skipped': skipped_files,
                    'current': query[:10] + '...' if len(query) > 10 else query
                })

        print(f"\nFinished processing: {processed_files} plans | Skipped: {skipped_files} test files")
        
        # Save the collected experience
        print("Saving experience to file...")
        with open(experience_file, 'wb') as f:
            pickle.dump((experience, baseline_plans), f)

    # Continue with training
    torch.manual_seed(SEED)
    random.seed(SEED)
    np.random.seed(SEED)
    
    if args.retrain and CHECKPOINT_PATH:
        print("Loading checkpoint and retraining...")
        neo = load_model(CHECKPOINT_PATH, config, env_config, device, experience, baseline_plans)
        checkpoint_episode = CHECKPOINT_PATH.split('_')[-1].split('.')[0]
        # Remove the ep characters
        checkpoint_episode = checkpoint_episode.replace('ep', '')
        checkpoint_episode = int(checkpoint_episode)
        neo.run(checkpoint_episode)
    else:
        agent = Agent(NeoTreeConvNet(**config['net_args']), collate_fn=collate,
                        device=config['neo_args']['device'])
        alg = Neo(agent, env_config, config['neo_args'],
                config['train_args'], experience, baseline_plans, parent_dir=parent_dir)
        alg.run()