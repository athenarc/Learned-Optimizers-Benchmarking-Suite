import os
import configparser
from dotenv import load_dotenv

def load_repo_env():
    """
    Walk up directories until the .env file is found (at repo root)
    and load it so environment variables become available.
    """
    current_dir = os.path.abspath(os.path.dirname(__file__))
    while True:
        env_path = os.path.join(current_dir, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
            break
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            raise FileNotFoundError("Could not find .env file in any parent directory.")
        current_dir = parent_dir

def read_config(cfg_name="bao.cfg"):
    """
    Reads the BAO configuration file and expands environment variables
    from .env inside values (e.g. ${DB_USER}, ${DB_PASS}, etc.)
    """
    load_repo_env()

    # Enable interpolation so ${VAR} placeholders get replaced by environment vars
    config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())

    # Resolve the config path relative to this script
    cfg_path = os.path.join(os.path.dirname(__file__), cfg_name)

    if not os.path.exists(cfg_path):
        raise FileNotFoundError(f"Configuration file not found: {cfg_path}")

    config.read(cfg_path)

    if "bao" not in config:
        raise ValueError(f"{cfg_name} does not have a [bao] section.")

    return config["bao"]
