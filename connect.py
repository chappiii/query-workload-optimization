import os
from pathlib import Path
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

DB_NAME = "recommendations"   

def get_driver():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    return driver

if __name__ == "__main__":
    driver = get_driver()
    
    with driver.session(database=DB_NAME) as session:
        result = session.run("MATCH (n) RETURN count(n) AS node_count")
        print(f"Connected to database: {DB_NAME}")
        print("Node count:", result.single()["node_count"])
    
    driver.close()
