import os
import re
import time
import random
import argparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from neo4j import GraphDatabase
from anonymizer import normalize_query

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
DB_NAME = "recommendations"

def get_driver():
    return GraphDatabase.driver(
        NEO4J_URI, 
        auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )

def clean_query(q: str):
    q = q.strip()
    q = re.sub(r"\s+", " ", q)  
    return q


def run_workload(mode= "raw", repeat_each=5):
    """
    mode = "raw"  → just clean query
    mode = "anon" → apply anonymization
    """
    driver = get_driver()

    with driver.session(database=DB_NAME) as session:
        session.run("CALL db.clearQueryCaches()")

    # Generate a massive workload with unique queries
    workload = []

    for i in range(50):

        workload.append(
            f'MATCH (actor{i}:Actor)-[:ACTED_IN]->(movie{i}:Movie) '
            f'RETURN actor{i}.name, movie{i}.title'
        )

        workload.append(
            'MATCH (a:Actor)-[:ACTED_IN]->(m:Movie) RETURN a.name, m.title'
        )

        workload.append(
            f'MATCH (m:Movie) WHERE m.title = "Movie{i}" RETURN m'
        )

        if i % 2 == 0:
            workload.append(
                f'MATCH (m:Movie {{year:{2000+i%20}, title:"Movie{i}"}}) RETURN m'
            )
        else:
            workload.append(
                f'MATCH (m:Movie {{title:"Movie{i}", year:{2000+i%20}}}) RETURN m'
            )

        workload.append(
            f'MATCH  (a:Actor) -[:ACTED_IN]-> (m:Movie)   '
            f'RETURN   a.name ,   m.title   LIMIT {i%10 + 1}'
        )

    random.shuffle(workload)


    log_dir = Path(__file__).parent / ("logs/anon" if mode == "anon" else "logs/raw")
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = log_dir / f"workload_log_{timestamp}.txt"

    log = []
    log.append(f"### Workload Run — {timestamp} ###")
    log.append(f"Total queries executed: {len(workload)}\n")

    with driver.session(database=DB_NAME) as session:
        for i, q in enumerate(workload, 1):
            if mode == "anon":
                q_processed = normalize_query(q)
            else:
                q_processed = clean_query(q)

            profiled_query = "PROFILE " + q_processed

            try:
                start = time.time()
                result = session.run(profiled_query)
                duration = time.time() - start
                list(result)
                summary = result.consume()

                # extract plan cache info
                args = summary.profile["args"]
                planning_ms = summary.result_available_after
                execution_ms = summary.result_consumed_after
                global_memory = summary.profile["args"].get("GlobalMemory")
                execution_ms = summary.result_consumed_after
                pure_execution_ms = max(0, execution_ms - planning_ms)
                rows = args.get("Rows", "?")
                db_hits = args.get("DbHits", "?")

                log.append(f"\n--- Query {i} ---")
                log.append("RAW: " + q)
                log.append("RUN: " + q_processed)
                log.append(f"Time: {duration:.4f}s")
                log.append(f"Planning: {planning_ms} ms")
                log.append(f"Execution: {execution_ms} ms")
                log.append(f"Pure Exec: {pure_execution_ms} ms")
                log.append(f"Memory: {global_memory}")
                log.append(f"Rows: {rows} | DB Hits: {db_hits}")
                log.append("-" * 50)



            except Exception as e:
                log.append(f"\nERROR on query {i}: {e}\n")

    driver.close()

    # Save logs
    with open(log_filename, "w", encoding="utf-8") as f:
        f.write("\n".join(log))

    print(f"\n Workload complete — log saved to: {log_filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["raw", "anon"], default="raw")
    args = parser.parse_args()

    run_workload(mode=args.mode, repeat_each=5)