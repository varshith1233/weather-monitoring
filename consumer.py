from confluent_kafka import Consumer, KafkaException
import pymongo
import json
import urllib.parse

    # Function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    # Function to check MongoDB connection
def check_mongodb_connection(mongo_uri):
    try:
        client = pymongo.MongoClient(mongo_uri)
        db = client.admin
        server_info = db.command('serverStatus')
        print("MongoDB connection successful.")
        return True
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return False

    # Function to consume weather data and save to MongoDB
def consume_and_save_to_mongodb(consumer, topic, mongo_uri):
    try:
        client = pymongo.MongoClient(mongo_uri)
        db = client["kafkaproj"]  # Your Database name
        collection = db["weather_collection"]  # Your mongo collection name

        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                        # End of partition event, not an error.
                    continue
                else:
                    print(msg.error())
                    break

            try:
                    # Decode the message value from Kafka and convert it to a Python dictionary
                weather_data = json.loads(msg.value().decode('utf-8'))
                print(weather_data)

                    # Drop the 'weather_icons' field if it exists
                if 'current' in weather_data and 'weather_icons' in weather_data['current']:
                    del weather_data['current']['weather_icons']

                    # Insert the modified data into MongoDB
                collection.insert_one(weather_data)

                print('Received message and saved to MongoDB:', weather_data)
            except (json.JSONDecodeError, TypeError) as e:
                print(f"Error decoding JSON: {e}")
                print('Received raw message:', msg.value())

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error in consumer: {e}")
    finally:
        consumer.close()

if check_mongodb_connection(mongo_uri):
        # Kafka consumer configuration
    consumer_conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'weather-consumer', 'auto.offset.reset': 'earliest'}
    consumer = Consumer(consumer_conf)

        # Kafka topic to consume from
    weather_topic = 'real_weather'

        # Consume and save to MongoDB
    consume_and_save_to_mongodb(consumer, weather_topic, mongo_uri)
else:
    print("MongoDB connection failed. Cannot proceed with data consumption.")

