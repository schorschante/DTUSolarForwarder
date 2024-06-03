# python 3.6
import random
import datetime
import  os, time
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
    "balkon_solar/114184914146/0/power": "current",
    "balkon_solar/114184914146/1/yieldday": "day",
    "balkon_solar/114184914146/0/yieldtotal": "total"
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
        influx_client = InfluxDBClient(url=url, token = os.environ.get("INFLUXDB_TOKEN"), org=org)
        today = datetime.datetime.now()
        bucket = "balkon_solar"

        print(today.strftime("%x"))
        print(today)
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

        write_api = influx_client.write_api(write_options=ASYNCHRONOUS)

        point = (
            Point(topic2db.get(msg.topic))
            .tag("date", today.strftime("%x"))
            .field("value", float(msg.payload.decode()))
        )

        write_api.write(bucket=bucket, org="schorschis", record=point)
        write_api.flush()
        write_api.close()
        influx_client.close()

    def deleteBuckets(bucket, influxClient):
        del_api = influxClient.delete_api()
        start = "1970-01-01T00:00:00Z"
        stop = "2025-02-01T00:00:00Z"
        del_api.delete(start, stop, '_measurement="current"', bucket=bucket, org=org)
        del_api.delete(start, stop, '_measurement="day"', bucket=bucket, org=org)
        del_api.delete(start, stop, '_measurement="total"', bucket=bucket, org=org)

    for topic_to_subscribe in topic2db.keys():
        client.subscribe(topic_to_subscribe)

    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
