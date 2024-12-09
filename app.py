import os
import re
import time
import json
import csv
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pytz import timezone
import requests
from bs4 import BeautifulSoup
from collections import Counter
import zopy.utils as zu

app = FastAPI(
    title="bioBakery Stats API",
    description="An API to fetch repository stats for the Biobakery organization from Docker Hub, Conda, PyPI, and Bioconductor.",
    version="1.0.0",
    contact={
        "name": "bioBakery Download Stats",
        "email": "sagunmaharjann@gmail.com",
    },
)

# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

JSON_FILE = "repository_stats.json"
CSV_FILE = "repository_stats.csv"

@app.get("/")
def read_root():
    return {"message": "Welcome to the Repository Stats API!"}

@app.get("/fetch-stats-from-file")
def fetch_stats_from_file(file_type: str = Query("json", enum=["json", "csv"])):
    stats = load_stats_from_file(file_type)
    if "error" in stats:
        raise HTTPException(status_code=404, detail=stats["error"])
    return stats

@app.get("/update-stats-from-api")
def update_stats_from_api(file_type: str = Query("json", enum=["json", "csv"])):
    docker_stats = fetch_all_docker_stats()
    conda_stats = fetch_conda_stats_via_curl()
    bioconductor_packages = ["MMUPHin", "Maaslin2", "Macarron", "banocc", "sparseDOSSA"]
    bioconductor_stats = fetch_bioconductor_stats(bioconductor_packages)

    combined_stats = {
        "docker": docker_stats,
        "conda": conda_stats,
        "bioconductor": bioconductor_stats,
    }

    save_stats_to_file(combined_stats, file_type)
    return {
        "message": f"Stats updated and saved to {file_type.upper()} file.",
        "last_update": datetime.now().isoformat(),
        "stats": combined_stats,
    }

def load_stats_from_file(file_type="json"):
    if file_type == "json":
        try:
            with open(JSON_FILE, "r") as json_file:
                return json.load(json_file)
        except FileNotFoundError:
            return {"error": "Stats file not found."}
    elif file_type == "csv":
        try:
            with open(CSV_FILE, "r") as csv_file:
                reader = csv.DictReader(csv_file)
                return [row for row in reader]
        except FileNotFoundError:
            return {"error": "Stats file not found."}

def save_stats_to_file(stats, file_type="json"):
    now = datetime.now(timezone("UTC"))
    est_time = now.astimezone(timezone("US/Eastern")).isoformat()
    if file_type == "json":
        with open(JSON_FILE, "w") as json_file:
            json.dump({"last_update": est_time, "stats": stats}, json_file, indent=4)
    elif file_type == "csv":
        with open(CSV_FILE, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["Platform", "Repository", "Pull Count", "Last Update"])
            for platform, platform_stats in stats.items():
                for repo, data in platform_stats.items():
                    writer.writerow([platform, repo, data.get("pull_count", "N/A"), est_time])

def fetch_all_docker_stats():
    repositories = fetch_biobakery_repositories()
    stats = {}
    for repository in repositories:
        print(f"Fetching stats for biobakery/{repository}...")
        pull_count = fetch_docker_stats(repository)
        stats[f"biobakery/{repository}"] = {"pull_count": pull_count} if pull_count else {"error": "Failed to fetch data"}
    return stats

def fetch_biobakery_repositories():
    repositories = []
    page = 1
    while True:
        url = f"https://hub.docker.com/v2/repositories/biobakery/?page={page}&page_size=100"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            repositories += [repo["name"] for repo in data["results"]]
            if not data["next"]:
                break
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching repositories: {e}")
            break
    return repositories

def fetch_docker_stats(repository):
    url = f"https://hub.docker.com/v2/repositories/biobakery/{repository}/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("pull_count", 0)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {repository}: {e}")
        return None

def fetch_conda_stats_via_curl():
    tools = set()
    c_pattern = r'<a data-package href="/biobakery/(.*?)">'
    os.system("curl https://anaconda.org/biobakery/repo --silent > TEMP.dat")

    for line in zu.iter_lines("TEMP.dat"):
        for M in re.finditer(c_pattern, line):
            tools.add(M.group(1))

    counts = Counter()
    counts[("biobakery", "*", "*")] = 0  # This is where the tuple key is used

    c_pattern = r'<span>(\d+)</span> total downloads'

    for tool in sorted(tools):
        print(f"Checking: {tool}")
        os.system(f"curl https://anaconda.org/biobakery/{tool} --silent > TEMP.dat")
        for line in zu.iter_lines("TEMP.dat"):
            for M in re.finditer(c_pattern, line):
                count = int(M.group(1).replace(",", ""))
                counts[("biobakery", "*", "*")] += count
                counts[("biobakery", tool, "*")] += count
        time.sleep(2)

    # Convert tuple keys to string for JSON compatibility
    string_counts = {}
    for key, value in counts.items():
        string_key = f"{key[0]}_{key[1]}_{key[2]}"  # Convert tuple to a string key
        string_counts[string_key] = value
    
    return {"conda": string_counts}


def fetch_bioconductor_stats(packages):
    # List of Bioconductor tools
    tools = [
        "banocc",
        "sparseDOSSA",
        "Maaslin2",
        "Macarron",
        "MMUPHin"
    ]

    counts = Counter()
    counts[("biobakery", "*", "*")] = 0  # Initialize total counter

    # Pattern to match the download count
    c_pattern = r'<span>(\d+)</span> total downloads'

    # Iterate through each tool and fetch stats
    for tool in sorted(tools):
        print(f"Checking: {tool}")
        os.system(f"curl https://bioconductor.org/packages/stats/bioc/{tool}/{tool}_stats.tab --silent > TEMP.dat")

        # Parse the downloaded data
        for line in zu.iter_rows("TEMP.dat"):
            if line[1] == "all":
                count = int(line[3])
                counts[("biobakery", "*", "*")] += count
                counts[("biobakery", tool, "*")] += count
        time.sleep(2)  # To avoid rate limits

    # Convert tuple keys to string for JSON compatibility
    string_counts = {}
    for key, value in counts.items():
        string_key = f"{key[0]}_{key[1]}_{key[2]}"  # Convert tuple to a string key
        string_counts[string_key] = value

    return {"bioconductor": string_counts}