# python 3.6

import random
import time
import queue
from paho.mqtt import client as mqtt_client
topic = "test/nfc"
client_id = f'python-mqtt-{random.randint(0, 1000)}'
q1 = queue.Queue()


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set("Device", "Polgara12ZED2122")
    client.on_connect = on_connect
    client.connect('mqtt.zig-web.com', 1883)
    return client


def Mqtt_queue(client):
    msg_count = 0
    while True:
        if (q1.empty()!=True):
            print("data available in queue")
            result = client.publish("98:CD:AC:51:4D:C4/nfc", q1.get())
        else:
            print("no data in queue")
        time.sleep(1)

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        Mqtt_log = msg.payload.decode("utf-8")
        # print(Mqtt_log.split(''))
        print(Mqtt_log)
        q1.put(Mqtt_log)

    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    client.loop_start()
    subscribe(client)
    Mqtt_queue(client)


if __name__ == '__main__':
    run()
