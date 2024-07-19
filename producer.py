import requests
from confluent_kafka import Producer
import json

def get_current_weather_data(api_key, city):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {'q': city, 'appid': api_key}
    response = requests.get(base_url, params=params)
    return response.json() if response.status_code == 200 else None

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_current_weather_data(api_key, cities, producer, topic):
    for city in cities:
        weather_data = get_current_weather_data(api_key, city)
        if weather_data:
            try:
                json_data = json.dumps(weather_data)
                
                producer.produce(topic,key=city, value=json_data, callback=delivery_report)
                producer.flush()
            except Exception as e:
                print(f"Error producing message: {e}")

producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)
weather_topic = 'real_weather'

with open(r"cities.txt", 'r') as file:
    cities = [line.strip() for line in file]

produce_current_weather_data(api_key, cities, producer, weather_topic)
