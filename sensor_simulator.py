import paho.mqtt.client as mqtt
import time
import random
import json
from datetime import datetime

broker = "broker.emqx.io"
port = 1883
my_name = "env_sensor_" + str(random.randint(1000, 9999))

print("Starting the Environmental Sensor Simulator...")
print("--------------------------------------------")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, my_name)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to the broker!")
    else:
        print("Failed to connect. Code: " + str(rc))

client.on_connect = on_connect

client.connect(broker, port, 60)
client.loop_start()

time.sleep(1)


room1 = {"bld": "A", "rm": "101", "type": "office"}
room2 = {"bld": "A", "rm": "102", "type": "lab"}
room3 = {"bld": "B", "rm": "201", "type": "conference"}

rooms = [room1, room2, room3]

print("Sending data... Press Ctrl+C to stop")

msg_count = 0

try:
    while True:
        for r in rooms:
            temp_val = round(20 + random.uniform(0, 10), 1)
            hum_val = round(40 + random.uniform(0, 30), 1)
            aq_val = random.randint(20, 80)
            
            now = datetime.now().isoformat()
            
            base_topic = "project/environment/" + r['bld'] + "/" + r['rm']
            
            temp_data = {
                "project": "environmental_monitoring",
                "sensor_id": r['bld'] + r['rm'] + "_temp",
                "value": temp_val,
                "unit": "C",
                "building": r['bld'],
                "room": r['rm'],
                "room_type": r['type'],
                "timestamp": now,
                "sensor_type": "temperature"
            }
            client.publish(base_topic + "/temperature", json.dumps(temp_data))
            print("Sent Temperature: " + str(temp_val))

            hum_data = {
                "project": "environmental_monitoring",
                "sensor_id": r['bld'] + r['rm'] + "_hum",
                "value": hum_val,
                "unit": "%",
                "building": r['bld'],
                "room": r['rm'],
                "room_type": r['type'],
                "timestamp": now,
                "sensor_type": "humidity"
            }
            client.publish(base_topic + "/humidity", json.dumps(hum_data))
            print("Sent Humidity: " + str(hum_val))

            aq_data = {
                "project": "environmental_monitoring",
                "sensor_id": r['bld'] + r['rm'] + "_aq",
                "value": aq_val,
                "unit": "AQI",
                "building": r['bld'],
                "room": r['rm'],
                "room_type": r['type'],
                "timestamp": now,
                "sensor_type": "airquality"
            }
            client.publish(base_topic + "/airquality", json.dumps(aq_data))
            print("Sent Air Quality: " + str(aq_val))
            
            print("--- Finished room " + r['rm'] + " ---")
            msg_count = msg_count + 3
            time.sleep(1)

        print("Waiting 10 seconds for next round...")
        time.sleep(10)

except KeyboardInterrupt:
    print("Stopping...")
    client.loop_stop()
    client.disconnect()
    print("Total messages sent: " + str(msg_count))