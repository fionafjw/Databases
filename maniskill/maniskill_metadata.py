import os
import json
import sqlite3
import pandas as pd
import requests

metadata_urls = [
    "https://example.com/maniskill_metadata1.json",
    "https://example.com/maniskill_metadata2.json"
]

# Download and save each file
for i, url in enumerate(metadata_urls, start=1):
    response = requests.get(url)
    if response.status_code == 200:
        filename = f"maniskill_metadata{i}.json"
        with open(filename, "wb") as file:
            file.write(response.content)
        print(f"Downloaded: {filename}")
    else:
        print(f"Failed to download {url} - Status Code: {response.status_code}")

# CONFIGURATION: Define metadata file paths
METADATA_FILES = ["maniskill_metadata1.json", "maniskill_metadata2.json"]  # Replace with actual file paths
DATABASE_NAME = "maniskill.db"

# FUNCTION TO LOAD METADATA FROM JSON FILES
def load_metadata(file_path):
    """ Load JSON metadata file """
    with open(file_path, "r") as f:
        return json.load(f)

# FUNCTION TO EXTRACT TASK INFORMATION
def extract_task_info(metadata):
    """ Extract task-related information from ManiSkill metadata """
    env_info = metadata.get("env_info", {})
    env_id = metadata.get("env_id", "")
    max_episode_steps = metadata.get("max_episode_steps", None)
    env_kwargs = metadata.get("env_kwargs", {})

    return {
        "env_id": env_id,
        "max_episode_steps": max_episode_steps,
        "env_kwargs": json.dumps(env_kwargs),  # Store as JSON string
    }

# FUNCTION TO EXTRACT EPISODE INFORMATION
def extract_episodes(metadata):
    """ Extract episodes and their attributes from ManiSkill metadata """
    episodes = metadata.get("episodes", [])
    parsed_episodes = []

    for episode in episodes:
        parsed_episodes.append({
            "episode_id": episode.get("episode_id", None),
            "reset_kwargs": json.dumps(episode.get("reset_kwargs", {})),  # Store as JSON string
            "control_mode": episode.get("control_mode", ""),
            "elapsed_steps": episode.get("elapsed_steps", None),
            "info": json.dumps(episode.get("info", {}))  # Store as JSON string
        })

    return parsed_episodes

# FUNCTION TO EXTRACT DATA SOURCE INFORMATION
def extract_source_info(metadata):
    """ Extract source type and description from ManiSkill metadata """
    return {
        "source_type": metadata.get("source_type", "unknown"),
        "source_desc": metadata.get("source_desc", "unknown")
    }

# FUNCTION TO SAVE DATA TO SQLITE DATABASE
def save_to_database(task_info, episodes, source_info):
    """ Save extracted data to an SQLite database """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # CREATE TABLES IF NOT EXIST
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS task_info (
        env_id TEXT PRIMARY KEY,
        max_episode_steps INTEGER,
        env_kwargs TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS episodes (
        episode_id INTEGER PRIMARY KEY,
        env_id TEXT,
        reset_kwargs TEXT,
        control_mode TEXT,
        elapsed_steps INTEGER,
        info TEXT,
        FOREIGN KEY(env_id) REFERENCES task_info(env_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS source_info (
        env_id TEXT PRIMARY KEY,
        source_type TEXT,
        source_desc TEXT,
        FOREIGN KEY(env_id) REFERENCES task_info(env_id)
    )
    """)

    # INSERT DATA INTO TABLES
    cursor.execute("INSERT OR REPLACE INTO task_info (env_id, max_episode_steps, env_kwargs) VALUES (?, ?, ?)",
                   (task_info["env_id"], task_info["max_episode_steps"], task_info["env_kwargs"]))

    cursor.execute("INSERT OR REPLACE INTO source_info (env_id, source_type, source_desc) VALUES (?, ?, ?)",
                   (task_info["env_id"], source_info["source_type"], source_info["source_desc"]))

    for episode in episodes:
        cursor.execute("INSERT OR REPLACE INTO episodes (episode_id, env_id, reset_kwargs, control_mode, elapsed_steps, info) VALUES (?, ?, ?, ?, ?, ?)",
                       (episode["episode_id"], task_info["env_id"], episode["reset_kwargs"], episode["control_mode"], episode["elapsed_steps"], episode["info"]))

    conn.commit()
    conn.close()
    print(f"Saved data for env_id: {task_info['env_id']}")

# MAIN EXECUTION: Process all metadata files
for file in METADATA_FILES:
    print(f"Processing: {file}")
    metadata = load_metadata(file)

    # Extract relevant information
    task_info = extract_task_info(metadata)
    episodes = extract_episodes(metadata)
    source_info = extract_source_info(metadata)

    # Save data into the database
    save_to_database(task_info, episodes, source_info)

print("Processing complete. Data stored in SQLite database.")
