import sqlite3

# File where query results will be saved
OUTPUT_FILE = "query_results.txt"

def save_query_results():
    """
    Query database and save results into text file.
    """
    conn = sqlite3.connect("maniskill.db")
    cursor = conn.cursor()

    with open(OUTPUT_FILE, "w") as f:
        # Get all unique environment IDs
        cursor.execute("SELECT DISTINCT env_id FROM task_info")
        env_ids = cursor.fetchall()

        f.write("=== Unique Environments in Database ===\n")
        for row in env_ids:
            f.write(f"{row[0]}\n")
        f.write("\n")

        # Query source info for each environment
        f.write("=== Source Info ===\n")
        cursor.execute("SELECT * FROM source_info")
        sources = cursor.fetchall()

        for source in sources:
            env_id, source_type, source_desc = source
            f.write(f"Env ID: {env_id}\n")
            f.write(f"   - Source Type: {source_type}\n")
            f.write(f"   - Source Description: {source_desc}\n\n")

        # Query each environment's episodes table and store 10 results
        for row in env_ids:
            env_id = row[0].replace("-", "_")  # Match table naming format
            f.write(f"\n=== Episodes for `{env_id}` (Showing up to 10) ===\n")

            # Count total episodes
            count_query = f"SELECT COUNT(*) FROM episodes_{env_id}"
            try:
                cursor.execute(count_query)
                total_episodes = cursor.fetchone()[0]
                f.write(f"Total Episodes: {total_episodes}\n")
            except sqlite3.OperationalError:
                f.write(f"Error: Table episodes_{env_id} not found.\n")
                continue

            # Store up to 10 episodes
            query = f"SELECT * FROM episodes_{env_id} LIMIT 10"
            try:
                cursor.execute(query)
                episodes = cursor.fetchall()

                for episode in episodes:
                    f.write(f"{episode}\n")

            except sqlite3.OperationalError as e:
                f.write(f"Error querying episodes_{env_id}: {str(e)}\n")

            f.write("\n")

    conn.close()
    print(f"Query results saved to {OUTPUT_FILE}")

# Run query and save
save_query_results()