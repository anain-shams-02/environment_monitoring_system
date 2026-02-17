import requests
import json
from requests.auth import HTTPBasicAuth

class Neo4jStorage:
    def __init__(self, uri, user, password, database="neo4j"):
        self.url = uri.rstrip('/')
        self.endpoint = self.url + "/db/" + database + "/query/v2"
        self.user = user
        self.pw = password
        self.auth = HTTPBasicAuth(user, password)
        
        test_query = {"statement": "RETURN 1"}
        r = requests.post(self.endpoint, json=test_query, auth=self.auth)
        if r.status_code == 200 or r.status_code == 202:
            print("Connected to Neo4j OK")
        else:
            print("Connection failed!")
            print(r.text)

    def save_reading(self, topic, payload):
        sid = payload['sensor_id']
        stype = payload['sensor_type']
        bld = payload['building']
        room = payload['room']
        val = payload['value']
        ts = payload['timestamp']
        unit = payload.get('unit', '')
        rtype = payload.get('room_type', 'unknown')
        

        query ="""
        MERGE (l:Location {id: $loc_id})
        SET l.building = $building, l.room = $room, l.room_type = $room_type
        MERGE (s:Sensor {id: $sensor_id})
        SET s.type = $sensor_type, s.unit = $unit
        MERGE (s)-[:LOCATED_IN]->(l)
        CREATE (r:Reading {
            id: $read_id,
            value: $value,
            timestamp: $timestamp,
            sensor_id: $sensor_id
        })
        MERGE (s)-[:HAS_READING]->(r)
        RETURN s
        """
        
        params = {
            "loc_id": bld + "-" + room,
            "building": bld,
            "room": room,
            "room_type": rtype,
            "sensor_id": sid,
            "sensor_type": stype,
            "unit": unit,
            "read_id": str(sid) + "_" + str(ts),
            "value": val,
            "timestamp": ts
        }
        
        try:
            res = requests.post(self.endpoint, json={"statement": query, "parameters": params}, auth=self.auth)
            if res.status_code == 200:
                print("Saved reading for " + sid)
        except Exception as e:
            print("Got an error saving reading:")
            print(e)

    def query_sensor_network(self):
        query = """
        MATCH (s:Sensor)-[:LOCATED_IN]->(l:Location)
        OPTIONAL MATCH (s)-[:HAS_READING]->(r)
        WITH s, l, r ORDER BY r.timestamp DESC
        RETURN s.id as sensor, s.type as type, 
               l.building as building, l.room as room,
               collect(r)[0].value as last_value,
               collect(r)[0].timestamp as last_time
        """
        r = requests.post(self.endpoint, json={"statement": query}, auth=self.auth)
        data = r.json()
        
        fields = data['data']['fields']
        values = data['data']['values']
        
        final_list = []
        for v in values:
            row_dict = {}
            for i in range(len(fields)):
                row_dict[fields[i]] = v[i]
            final_list.append(row_dict)
            
        return final_list

    def get_location_summary(self):
        query = """
        MATCH (s:Sensor)-[:LOCATED_IN]->(l:Location)
        RETURN l.building + '-' + l.room as location, 
               count(s) as sensor_count,
               collect(s.type) as sensor_types
        """
        r = requests.post(self.endpoint, json={"statement": query}, auth=self.auth)
        data = r.json()
        
        fields = data['data']['fields']
        values = data['data']['values']
        
        results = []
        for v in values:
            item = {
                fields[0]: v[0],
                fields[1]: v[1],
                fields[2]: v[2]
            }
            results.append(item)
        return results

    def close(self):
        print("Closing...")