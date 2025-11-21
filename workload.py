import os
import random
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from neo4j import GraphDatabase
import time
import re

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


def run_workload(repeat_each=5):
    driver = get_driver()

    # Generate a massive workload with unique queries
    workload = []
    for i in range(100):
        # Unique literal value
        workload.append(f'MATCH (m:Movie) WHERE m.title = "Movie{i}" RETURN m')
        # Unique variable names
        workload.append(f'MATCH (actor{i}:Actor)-[:ACTED_IN]->(movie{i}:Movie) RETURN actor{i}.name, movie{i}.title')
        # Unique property order
        workload.append(f'MATCH (m:Movie {{year:{2000+i%20}, title:"Movie{i}"}}) RETURN m')
        # Unique formatting
        workload.append(f'MATCH (a:Actor)-[:ACTED_IN]->(m:Movie) RETURN a.name, m.title LIMIT {i%10+1}')
        # Unique aliasing
        workload.append(f'MATCH (a:Actor)-[:ACTED_IN]->(m:Movie) RETURN a.name AS actorName{i}, m.title AS movieTitle{i} LIMIT 5')

    random.shuffle(workload)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True) 
    log_filename = log_dir / f"workload_log_{timestamp}.txt"

    log = []
    log.append(f"### Workload Run — {timestamp} ###")
    log.append(f"Total queries executed: {len(workload)}\n")

    with driver.session(database=DB_NAME) as session:
        for i, q in enumerate(workload, 1):
            q_clean = clean_query(q)
            profiled_query = "PROFILE " + q_clean

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
                pure_execution_ms = execution_ms - planning_ms
                rows = args.get("Rows", "?")
                db_hits = args.get("DbHits", "?")

                log.append(f"\n--- Query {i} ---")
                log.append(q_clean)
                log.append(f"Total Time (wall): {duration:.4f}s")
                log.append(f"Planning: {planning_ms} ms")
                log.append(f"Execution: {execution_ms} ms")
                log.append(f"Memory: {global_memory}")
                log.append(f"Rows: {rows} | DB Hits: {db_hits}")
                log.append(f"exe: {pure_execution_ms} ms")
                log.append("-" * 50)



            except Exception as e:
                log.append(f"\nERROR on query {i}: {e}\n")

    driver.close()

    # Save logs
    with open(log_filename, "w", encoding="utf-8") as f:
        f.write("\n".join(log))

    print(f"\n Workload complete — log saved to: {log_filename}")


if __name__ == "__main__":
    run_workload(repeat_each=5)   