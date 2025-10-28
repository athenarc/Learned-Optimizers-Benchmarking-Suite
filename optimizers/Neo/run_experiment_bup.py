import shutil
import random
import sys
import yaml
from database_env import *
from algorithm.neo import *
from model import *
from tqdm import tqdm
import pickle

SEED = 123

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)


if __name__ == '__main__':
    mp.set_start_method("spawn")
    np.random.seed(123)
    torch.manual_seed(123)

    config_path = sys.argv[1]
    env_config_path = sys.argv[2]
    with open(config_path, 'r') as file:
        d = yaml.load(file, Loader=yaml.FullLoader)
    with open(env_config_path, "r") as f:
        env_config = json.load(f)
    Path(d['neo_args']['logdir']).mkdir(parents=True, exist_ok=True)
    shutil.copy(config_path, Path(d['neo_args']['logdir']) / 'config.yaml')
    env_config['return_latency'] = d['neo_args']['latency']

    test_set = env_config['test_queries']
    env_config['db_data'] = {
        k: v for k, v in env_config['db_data'].items() if not k in test_set}

    experience_file = Path(d['neo_args']['logdir']) / 'saved_experience.pkl'
    # create_agent
    agent = Agent(NeoTreeConvNet(**d['net_args']), collate_fn=collate,
                    device=d['neo_args']['device'])

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
        path_plan = Path(d['neo_args']['baseline_path'])
        baseline_plans = {}
        # Initialize counters
        total_files = len(list(path_plan.glob("*.sql.json")))
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
                
                if query in test_set:
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
    alg = Neo(agent, env_config, d['neo_args'],
              d['train_args'], experience, baseline_plans)
    alg.run()