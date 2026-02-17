import json
from database_sqlite import SQLiteStorage
from database_mongodb import MongoDBStorage
from database_neo4j_http import Neo4jStorage

mongo_url = "mongodb+srv://Anain:Aceraspire%403@cluster0.ktrxmih.mongodb.net/?appName=Cluster0"
neo_url = "https://c7363e80.databases.neo4j.io"
neo_user = "neo4j"
neo_pass = "N_BJ-mhGokktNjzCo6F6wrqXYl6bHbqOXvUXNcKxs4k"
sqlite_file = "environmental_data.db"

print("--- CHECKING MY SENSOR DATA ---")

try:
    db1 = SQLiteStorage(sqlite_file)
    print("\nLooking at SQLite data...")

    cur = db1.connection.cursor()
    
    cur.execute("SELECT timestamp, building, room, value FROM temperature ORDER BY timestamp DESC LIMIT 3")
    temp_rows = cur.fetchall()
    print("Recent Temps:")
    for r in temp_rows:
        print("Time: " + str(r[0]) + " | Val: " + str(r[3]) + " at " + str(r[1]) + "-" + str(r[2]))

    cur.execute("SELECT timestamp, building, room, value FROM humidity ORDER BY timestamp DESC LIMIT 3")
    hum_rows = cur.fetchall()
    print("\nRecent Humidity:")
    for r in hum_rows:
        print("Time: " + str(r[0]) + " | Val: " + str(r[3]) + " at " + str(r[1]) + "-" + str(r[2]))

    db1.close()
except Exception as e:
    print("SQLite error happened: " + str(e))

try:
    db2 = MongoDBStorage(mongo_url)
    print("\n\nLooking at MongoDB data...")

    all_latest = db2.get_latest_readings(3)

    print("Last 3 Temps:")
    for d in all_latest['temperature']:
        print("Val: " + str(d.get('value')) + " in " + str(d.get('building')))

    print("\nLast 3 Humidity:")
    for d in all_latest['humidity']:
        print("Val: " + str(d.get('value')) + " in " + str(d.get('building')))

    db2.close()
except Exception as e:
    print("MongoDB error happened: " + str(e))

try:
    db3 = Neo4jStorage(neo_url, neo_user, neo_pass)
    print("\n\nLooking at Neo4j Graph...")

    net = db3.query_sensor_network()
    print("Where are my sensors?")
    for s in net:
        name = s['sensor']
        b = s['building']
        val = s.get('last_value')
        print("Sensor " + str(name) + " is in " + str(b) + ". Last value was: " + str(val))

    locs = db3.get_location_summary()
    print("\nRoom Summary:")
    for l in locs:
        print("Room: " + str(l['location']) + " has " + str(l['sensor_count']) + " sensors")

    db3.close()
except Exception as e:
    print("Neo4j error happened: " + str(e))

print("\nFinished checking everything!")