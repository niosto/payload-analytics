import json
import requests
import os

# Task 1 helper: Download raw JSON from S3
def download_s3(params: dict):
    """Download a JSON file from a public S3 URL and persist it locally."""
    url = params["url"]
    data = requests.get(url).text
    path = params["path"]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(data)