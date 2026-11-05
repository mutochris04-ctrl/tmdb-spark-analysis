"""Step 1: Fetch movie data from the TMDB API and save it as a JSON file."""
import json
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()  # reads TMDB_API_KEY from the .env file

API_KEY = os.environ.get("TMDB_API_KEY")
if not API_KEY:
    raise ValueError("TMDB API key is missing! Put TMDB_API_KEY=... in your .env file.")
BASE_URL = "https://api.themoviedb.org/3/movie/"
RAW_FILE = "data/raw_movies.json"

MOVIE_IDS = [0, 299534, 19995, 140607, 299536, 597, 135397,
             420818, 24428, 168259, 99861, 284054, 12445,
             181808, 330457, 351286, 109445, 321612, 260513]


def fetch_movie(movie_id, max_tries=3):
    """Fetch one movie (with its credits). Retry with a growing wait if it fails."""
    url = BASE_URL + str(movie_id)
    params = {"api_key": API_KEY, "append_to_response": "credits"}

    for attempt in range(1, max_tries + 1):
        try:
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                return response.json()
            if response.status_code == 404:
                print("Movie", movie_id, "not found - skipped")
                return None

            print("Movie", movie_id, "got status", response.status_code)
        except requests.RequestException as error:
            print("Movie", movie_id, "error:", error)

        if attempt < max_tries:  # no need to wait after the last try
            wait = 2 ** attempt  # backoff: wait 2, then 4 seconds
            print("Trying again in", wait, "seconds...")
            time.sleep(wait)

    print("Movie", movie_id, "failed after", max_tries, "tries")
    return None


def fetch_all_movies():
    """Fetch all movies in MOVIE_IDS and save them, one movie per line."""
    os.makedirs("data", exist_ok=True)
    movies = []

    for movie_id in MOVIE_IDS:
        movie = fetch_movie(movie_id)
        if movie is not None:
            movies.append(movie)

    with open(RAW_FILE, "w", encoding="utf-8") as f:
        for movie in movies:
            f.write(json.dumps(movie) + "\n")

    print("Saved", len(movies), "movies to", RAW_FILE)
    return RAW_FILE


if __name__ == "__main__":
    fetch_all_movies()
