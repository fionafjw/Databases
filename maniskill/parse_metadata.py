import os
import json
import sqlite3


# File containing JSON paths
JSON_PATHS_FILE = "json_paths.txt"

# Database name
DATABASE_NAME = "maniskill.db"


# Function to load JSON metadata
def load_metadata(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def extract_task_info(metadata):
    """Extracts task-level metadata (env_id, max_episode_steps, env_kwargs)."""
    env_info = metadata.get("env_info", {})
    env_id = env_info.get("env_id", "UnknownTask")
    max_episode_steps = env_info.get("max_episode_steps", None)
    env_kwargs = json.dumps(env_info.get("env_kwargs", {}))

    return {"env_id": env_id, "max_episode_steps": max_episode_steps, "env_kwargs": env_kwargs}

def extract_source_info(metadata):
    """Extracts source type and description."""
    env_id = metadata.get("env_info", {}).get("env_id", "UnknownTask")
    source_type = metadata.get("source_type", "Unknown")
    source_desc = metadata.get("source_desc", "No description")

    return {"env_id": env_id, "source_type": source_type, "source_desc": source_desc}

def extract_episodes(metadata, env_id):
    """Extracts episodes and formats them for insertion."""
    episodes = metadata.get("episodes", [])
    parsed_episodes = []

    for episode in episodes:
        parsed_episodes.append({
            "episode_id": episode.get("episode_id"),
            "env_id": env_id,
            "episode_seed": episode.get("episode_seed"),
            "reset_kwargs": json.dumps(episode.get("reset_kwargs", {})),  # Convert dict to JSON string
            "control_mode": episode.get("control_mode", ""),
            "elapsed_steps": episode.get("elapsed_steps", 0),
            "success": int(episode.get("success", False)),
            "fail": int(episode.get("fail", False))
        })

    return parsed_episodes


def create_database():
    """Creates database tables if they do not exist."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Create tables for metadata storage
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS task_info (
            env_id TEXT PRIMARY KEY,
            max_episode_steps INTEGER,
            env_kwargs TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS source_info (
            env_id TEXT PRIMARY KEY,
            source_type TEXT,
            source_desc TEXT
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Database schema initialized.")


def save_to_database(task_info, episodes, source_info):
    conn = sqlite3.connect("maniskill.db")
    cursor = conn.cursor()

    env_id = task_info["env_id"]

    # Store Task Info
    cursor.execute("""
        INSERT OR REPLACE INTO task_info (env_id, max_episode_steps, env_kwargs) 
        VALUES (?, ?, ?)
    """, (env_id, task_info["max_episode_steps"], task_info["env_kwargs"]))

    # Store Source Info
    cursor.execute("""
        INSERT OR REPLACE INTO source_info (env_id, source_type, source_desc) 
        VALUES (?, ?, ?)
    """, (env_id, source_info["source_type"], source_info["source_desc"]))

    # Sanitize `env_id` for safe SQLite table naming
    safe_env_id = env_id.replace("-", "_")

    # Create Unique Table for Each `env_id`
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS episodes_{safe_env_id} (
            episode_id INTEGER PRIMARY KEY,
            episode_seed INTEGER,
            reset_kwargs TEXT,
            control_mode TEXT,
            elapsed_steps INTEGER,
            success INTEGER,
            fail INTEGER
        )
    """)

    # Insert Episodes
    for episode in episodes:
        reset_kwargs = episode.get("reset_kwargs", {})  # Default to empty dict if missing
        control_mode = episode.get("control_mode", "")

        # Convert `reset_kwargs` and `control_mode` to JSON strings if needed
        reset_kwargs_str = json.dumps(reset_kwargs) if isinstance(reset_kwargs, dict) else str(reset_kwargs)
        control_mode_str = json.dumps(control_mode) if isinstance(control_mode, dict) else str(control_mode)

        cursor.execute(f"""
            INSERT OR REPLACE INTO episodes_{safe_env_id} (
                episode_id, episode_seed, reset_kwargs, control_mode, elapsed_steps, success, fail
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            episode["episode_id"], episode["episode_seed"], reset_kwargs_str,
            control_mode_str, episode["elapsed_steps"], episode["success"], episode["fail"]
        ))

    conn.commit()
    conn.close()
    print(f"✅ Saved {len(episodes)} episodes for env_id: {env_id}")


def process_json_paths():
    """ Reads JSON paths from json_paths.txt and processes each file. """
    with open("json_paths.txt", "r") as f:
        json_files = [line.strip() for line in f.readlines()]

    for file_path in json_files:
        print(f"\nProcessing JSON File: {file_path} ...")

        if os.path.exists(file_path) and file_path.endswith(".json"):
            with open(file_path, "r") as json_file:
                metadata = json.load(json_file)

            # Extract Data
            task_info = extract_task_info(metadata)
            source_info = extract_source_info(metadata)
            episodes = extract_episodes(metadata, task_info["env_id"])

            # Save to DB
            save_to_database(task_info, episodes, source_info)

# Run the script
if __name__ == "__main__":
    create_database()
    process_json_paths()
    print("All metadata files processed successfully!")