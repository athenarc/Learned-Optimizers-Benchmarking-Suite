import sys
from database_env import *
from algorithm.neo import *
from model import *

SEED = 123

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

if __name__ == '__main__':
    mp.set_start_method("spawn")
    np.random.seed(123)
    torch.manual_seed(123)

    env_config_path = '/data/hdd1/users/kmparmp/Neo/config/postgres_job_config.json'
    with open(env_config_path, "r") as f:
        env_config = json.load(f)

    test_set = env_config['test_queries']
    env_config['db_data'] = {
        k: v for k, v in env_config['db_data'].items() if not k in test_set}
    
    output_dir = Path("runs/job_added_index/postgresql/optimizer")        
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    build_and_save_optimizer_plans(env_config, output_dir)

