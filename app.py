from fastapi import FastAPI, HTTPException
import requests

app = FastAPI()

# Function to fetch all repositories for the biobakery organization
def fetch_biobakery_repositories():
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


# Function to fetch Docker stats from Docker Hub API
def fetch_docker_stats(repository):
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


# Function to get stats for all repositories in biobakery
def fetch_all_docker_stats():
    repositories = fetch_biobakery_repositories()
    stats = {}
    for repository in repositories:
        print(f"Fetching stats for biobakery/{repository}...")
        pull_count = fetch_docker_stats(repository)
        if pull_count is not None:
            stats[f"biobakery/{repository}"] = {"pull_count": pull_count}
        else:
            stats[f"biobakery/{repository}"] = {"error": "Failed to fetch data"}
    return stats


@app.get("/")
def read_root():
    return {"message": "Welcome to the Docker Stats API!"}


@app.get("/get-docker-stats")
def get_docker_stats():
    """
    Fetch the Docker pull counts for all repositories in the biobakery organization.
    """
    stats = fetch_all_docker_stats()
    if not stats:
        raise HTTPException(status_code=500, detail="Failed to fetch stats.")
    return stats
