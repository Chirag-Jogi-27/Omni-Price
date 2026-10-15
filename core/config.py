import os
import json
from dotenv import load_dotenv

load_dotenv()

def get_proxy_config():
    proxy_server = os.getenv("PROXY_SERVER")
    if proxy_server and "your-proxy-domain" not in proxy_server:
        return {
            "server": proxy_server,
            "username": os.getenv("PROXY_USERNAME"),
            "password": os.getenv("PROXY_PASSWORD")
        }
    return None

def get_targets():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'targets.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        raise FileNotFoundError(f"Missing config/targets.json: {e}")
