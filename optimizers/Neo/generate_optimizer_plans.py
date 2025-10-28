import sys
from database_env import *
from algorithm.neo import *
from model import *
from pathlib import Path
REPO_ROOT = Path(__file__).resolve()
while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
    REPO_ROOT = REPO_ROOT.parent

SEED = 123

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

if __name__ == '__main__':
    mp.set_start_method("spawn")
    np.random.seed(123)
    torch.manual_seed(123)

    env_config_path = f'{REPO_ROOT}/optimizers/Neo/config/postgres_job_config.json'
    with open(env_config_path, "r") as f:
        env_config = json.load(f)

    test_set = env_config['test_queries']
    env_config['db_data'] = {
        k: v for k, v in env_config['db_data'].items() if not k in test_set}
    
    output_dir = Path("runs/job_added_index/postgresql/optimizer")        
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    build_and_save_optimizer_plans(env_config, output_dir)

