from fastapi import FastAPI, HTTPException, Query
import json
from datetime import datetime
import csv
import requests
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="bioBakery Stats API",
    description="An API to fetch Docker Hub repository stats for the Biobakery organization.",
    version="1.0.0",
    contact={
        "name": "bioBakery Download Stats",
        "email": "your-email@example.com",
    },
)
# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace with specific origins like ["http://localhost:3000"] for security
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)


JSON_FILE = "docker_stats.json"
CSV_FILE = "docker_stats.csv"


# Function definitions as in the previous implementation...


@app.get(
    "/",
    summary="Welcome Message",
    description="Returns a welcome message for the Docker Stats API.",
    tags=["General"],
)
def read_root():
    return {"message": "Welcome to the Docker Stats API!"}


@app.get(
    "/fetch-stats-from-file",
    summary="Fetch Stats from File",
    description="Fetches Docker Hub repository stats from a local JSON/CSV file, including the last update date/time.",
    tags=["Stats"],
)
def fetch_stats_from_file(file_type: str = Query("json", enum=["json", "csv"], description="The file format to read stats from (json or csv).")):
    """
    Fetch stats from the local file.

    - **file_type**: Specify whether to fetch data from a JSON or CSV file.
    """
    stats = load_stats_from_file(file_type)
    if "error" in stats:
        raise HTTPException(status_code=404, detail=stats["error"])
    return stats


@app.get(
    "/update-stats-from-api",
    summary="Update Stats from Docker API",
    description="Fetches the latest Docker Hub repository stats from the Docker API and saves them to a local JSON/CSV file.",
    tags=["Stats"],
)
def update_stats_from_api(file_type: str = Query("json", enum=["json", "csv"], description="The file format to save stats to (json or csv).")):
    """
    Update stats by fetching data from Docker API.

    - **file_type**: Specify whether to save data in a JSON or CSV file.
    """
    stats = fetch_all_docker_stats()
    if not stats:
        raise HTTPException(status_code=500, detail="Failed to fetch stats from API.")
    save_stats_to_file(stats, file_type)
    return {"message": f"Stats updated and saved to {file_type.upper()} file.", "last_update": datetime.now().isoformat()}

def load_stats_from_file(file_type="json"):
    """
    Load stats from the local JSON or CSV file.
    """
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
    """
    Save stats to a JSON or CSV file with a timestamp.
    """
    now = datetime.now().isoformat()
    if file_type == "json":
        with open(JSON_FILE, "w") as json_file:
            json.dump({"last_update": now, "stats": stats}, json_file, indent=4)
    elif file_type == "csv":
        with open(CSV_FILE, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["Repository", "Pull Count", "Last Update"])
            for repo, data in stats.items():
                writer.writerow([repo, data.get("pull_count", "N/A"), now])


def fetch_all_docker_stats():
    """
    Fetch Docker stats for all repositories in the Biobakery organization and sort by pull count in descending order.
    """
    repositories = fetch_biobakery_repositories()
    stats = {}
    for repository in repositories:
        print(f"Fetching stats for biobakery/{repository}...")
        pull_count = fetch_docker_stats(repository)
        if pull_count is not None:
            stats[f"biobakery/{repository}"] = {"pull_count": pull_count}
        else:
            stats[f"biobakery/{repository}"] = {"error": "Failed to fetch data"}
    
    # Sort stats by pull count in descending order
    sorted_stats = dict(
        sorted(stats.items(), key=lambda item: item[1].get("pull_count", 0), reverse=True)
    )
    
    return sorted_stats



def fetch_biobakery_repositories():
    """
    Fetch a list of all repositories for the Biobakery organization.
    """
    repositories = []
    page = 1
    while True:
        url = f"https://hub.docker.com/v2/repositories/biobakery/?page={page}&page_size=100"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Add repositories to the list
            repositories += [repo["name"] for repo in data["results"]]

            # Check if there are more pages
            if not data["next"]:
                break
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching repositories: {e}")
            break
    return repositories


def fetch_docker_stats(repository):
    """
    Fetch stats for a specific Docker repository.
    """
    url = f"https://hub.docker.com/v2/repositories/biobakery/{repository}/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Extract pull count
        pull_count = data.get("pull_count", 0)
        return pull_count
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {repository}: {e}")
        return None
