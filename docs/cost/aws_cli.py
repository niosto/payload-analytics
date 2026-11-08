"""Helper: run read-only AWS CLI calls with credentials loaded from .envs/aws.env (never printed)."""
import json
import os
import subprocess
from pathlib import Path

ENV_FILE = Path(os.environ.get(
    "AWS_ENV_FILE",
    r"C:/Users/ASUS/Downloads/payload-analytics-main/payload-analytics-main/.envs/aws.env"))


def _env():
    env = dict(os.environ)
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"')
    return env


def aws(*args):
    out = subprocess.run(["aws", *args, "--output", "json"], env=_env(),
                         capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip()[:500])
    return json.loads(out.stdout) if out.stdout.strip() else {}


def products(service, filters, max_items=100):
    """Price List API query; returns list of parsed product dicts."""
    args = ["pricing", "get-products", "--region", "us-east-1", "--service-code", service,
            "--max-items", str(max_items)]
    # all filters must follow a single --filters flag (repeating the flag keeps only the last one)
    args += ["--filters"] + [f"Type=TERM_MATCH,Field={k},Value={v}" for k, v in filters.items()]
    res = aws(*args)
    return [json.loads(p) if isinstance(p, str) else p for p in res.get("PriceList", [])]


def od_prices(prod):
    """Yield (unit, description, usd, begin, end) of OnDemand dimensions."""
    for term in prod.get("terms", {}).get("OnDemand", {}).values():
        for d in term["priceDimensions"].values():
            yield (d["unit"], d["description"], float(d["pricePerUnit"].get("USD", 0)),
                   d.get("beginRange"), d.get("endRange"))
