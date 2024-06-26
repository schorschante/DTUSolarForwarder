# python 3.6
import random
import datetime
import json
import os
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from paho.mqtt import client as mqtt_client
token = os.environ.get("INFLUXDB_TOKEN")
org = "schorschis"
url = "http://octopi.local:8086"
broker = '192.168.178.192'
port = 1883
topic = "#"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
topic2db = {
    "sensor/moisture": "plant_moisture"
}


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", reason_code)

    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2, client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        influx_client = InfluxDBClient(url=url, token = "IFRBxTGvyup3PJIW1n-a_fnRS4UYY09KQtRKUhK8eFpsMZteNNRRhqf85uk3QHDRgH9OAk2IQP15FZ-eG25Apg==")
        today = datetime.datetime.now()


        print(today.strftime("%x"))
        print(today)
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

        writeToInflux(influx_client, msg)

        #deleteBuckets("balkon_solar",influx_client)

    def writeToInflux(influx_client, msg):
        today = datetime.datetime.now()
        bucket = "plant_moisture"
        json_dict = json.loads(msg.payload.decode('utf-8'))

        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        point = Point(topic2db.get(msg.topic))
        tag_item = json_dict.popitem()
        point = point.tag(tag_item[0], tag_item[1])
        point = point.field(tag_item[0], tag_item[1])

        for key, value in json_dict.items():
            point = point.field(key, float(value))
            print(key, value)
        write_api.write(bucket=bucket, org="schorschis", record=point)
        influx_client.close()

    def deleteBuckets(bucket, influxClient):
        del_api = influxClient.delete_api()
        start = "1970-01-01T00:00:00Z"
        stop = "2025-02-01T00:00:00Z"
        del_api.delete(start, stop, '_measurement="current"', bucket=bucket, org=org)
        del_api.delete(start, stop, '_measurement="day"', bucket=bucket, org=org)
        del_api.delete(start, stop, '_measurement="total"', bucket=bucket, org=org)
        del_api.delete(start, stop, '_measurement="deepsleep"', bucket=bucket, org=org)

    for topic_to_subscribe in topic2db.keys():
        client.subscribe(topic_to_subscribe)

    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()

