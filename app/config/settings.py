import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


def _deep_merge(base, overrides):
    out = dict(base)
    for k, v in overrides.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def get_config():
    default_path = Path(__file__).resolve().parents[2] / "config" / "default.yaml"
    path = Path(os.getenv("CONFIG_PATH", str(default_path)))
    with path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    env_overrides = {"postgres": {}, "clickhouse": {}}
    if os.getenv("POSTGRES_HOST"):
        env_overrides["postgres"]["host"] = os.environ["POSTGRES_HOST"]
    if os.getenv("POSTGRES_PORT"):
        env_overrides["postgres"]["port"] = int(os.environ["POSTGRES_PORT"])
    if os.getenv("POSTGRES_DB"):
        env_overrides["postgres"]["database"] = os.environ["POSTGRES_DB"]
    if os.getenv("POSTGRES_USER"):
        env_overrides["postgres"]["user"] = os.environ["POSTGRES_USER"]
    if os.getenv("POSTGRES_PASSWORD"):
        env_overrides["postgres"]["password"] = os.environ["POSTGRES_PASSWORD"]

    if os.getenv("CLICKHOUSE_HOST"):
        env_overrides["clickhouse"]["host"] = os.environ["CLICKHOUSE_HOST"]
    if os.getenv("CLICKHOUSE_PORT"):
        env_overrides["clickhouse"]["port"] = int(os.environ["CLICKHOUSE_PORT"])
    if os.getenv("CLICKHOUSE_DB"):
        env_overrides["clickhouse"]["database"] = os.environ["CLICKHOUSE_DB"]
    if os.getenv("CLICKHOUSE_USER"):
        env_overrides["clickhouse"]["user"] = os.environ["CLICKHOUSE_USER"]
    if os.getenv("CLICKHOUSE_PASSWORD"):
        env_overrides["clickhouse"]["password"] = os.environ["CLICKHOUSE_PASSWORD"]

    for section, vals in env_overrides.items():
        if vals:
            cfg = _deep_merge(cfg, {section: vals})

    return cfg
