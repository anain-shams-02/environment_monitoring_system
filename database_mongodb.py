import pymongo
from datetime import datetime

class MongoDBStorage:
    def __init__(self, connection_string):
        self.client = pymongo.MongoClient(connection_string)
        self.db = self.client["environmental_monitoring"]
        
        self.temperature = self.db["temperature"]
        self.humidity = self.db["humidity"]
        self.airquality = self.db["airquality"]
        
        print("Connected to MongoDB!")

    def save_reading(self, topic, payload):
        payload['received_at'] = datetime.now().isoformat()
        
        if 'temperature' in topic:
            self.temperature.insert_one(payload)
        elif 'humidity' in topic:
            self.humidity.insert_one(payload)
        elif 'airquality' in topic:
            self.airquality.insert_one(payload)
            
    def get_latest_readings(self, limit=5):
        results = {}
        
        results['temperature'] = list(self.temperature.find({}, {'_id': 0}).sort('timestamp', -1).limit(limit))
        results['humidity'] = list(self.humidity.find({}, {'_id': 0}).sort('timestamp', -1).limit(limit))
        results['airquality'] = list(self.airquality.find({}, {'_id': 0}).sort('timestamp', -1).limit(limit))
        
        return results

    def close(self):
        self.client.close()
        print("MongoDB connection closed")