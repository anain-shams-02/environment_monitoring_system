import paho.mqtt.client as mqtt
import json
import time
from database_sqlite import SQLiteStorage
from database_mongodb import MongoDBStorage
from database_neo4j_http import Neo4jStorage

broker_address = "broker.emqx.io"
port = 1883
my_id = "my_subscriber_test_123"

mongo_url = "mongodb+srv://Anain:Aceraspire%403@cluster0.ktrxmih.mongodb.net/?appName=Cluster0"
neo_url = "https://c7363e80.databases.neo4j.io"
neo_user = "neo4j"
neo_pass = "N_BJ-mhGokktNjzCo6F6wrqXYl6bHbqOXvUXNcKxs4k"

print("Starting the Environmental Monitor...")
print("--------------------------------------")

print("Connecting to databases...")
db_sql = SQLiteStorage("environmental_data.db")
db_mongo = MongoDBStorage(mongo_url)
db_neo = Neo4jStorage(neo_url, neo_user, neo_pass)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT!")
        client.subscribe("project/environment/#")
    else:
        print("Failed to connect, error code: " + str(rc))

def on_message(client, userdata, msg):
    try:
        raw_data = msg.payload.decode()
        data = json.loads(raw_data)
        
        if data['project'] == 'environmental_monitoring':
            print("Received a new reading:")
            print("Sensor: " + str(data['sensor_id']))
            print("Value: " + str(data['value']))
        
            db_sql.save_reading(msg.topic, msg.payload)
            db_mongo.save_reading(msg.topic, data)
            db_neo.save_reading(msg.topic, data)
            
            print("Saved to all databases.")
            print("---")
            
    except Exception as e:
        print("Error in on_message: " + str(e))

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, my_id)
client.on_connect = on_connect
client.on_message = on_message

print("Connecting to broker: " + broker_address)
client.connect(broker_address, port, 60)

try:
    print("Waiting for data... Press Ctrl+C to quit")
    client.loop_forever()
    
except KeyboardInterrupt:
    print("Stopping the script...")
    
    print("Graph Network Summary:")
    net = db_neo.query_sensor_network()
    for s in net:
        print("Sensor: " + s['sensor'] + " in " + s['building'])
        
    print("Closing connections...")
    db_sql.close()
    db_mongo.close()
    db_neo.close()
    client.disconnect()
    print("Done.")