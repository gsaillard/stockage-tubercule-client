# python 3.6


import json
import logging
import random
import time
import csv

from paho.mqtt import client as mqtt_client

filename = 'log.csv'
BROKER = 'localhost'
PORT = 1883
TOPIC = "zigbee2mqtt/+"
# generate client ID with pub prefix randomly
CLIENT_ID = f'python-mqtt-tcp-pub-sub-{random.randint(0, 1000)}'
USERNAME = ''
PASSWORD = ''


FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60

last_message = dict()

FLAG_EXIT = False

with open(filename, 'w') as csvfile:
    def subscribe(client: mqtt_client):
        def on_message(client, userdata, msg):
            print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
            mydict = json.loads(msg.payload.decode())
            mydict["topic"] = str(msg.topic)
            mydict["time"] = time.time()
            if str(msg.topic) in last_message:
                if last_message[str(msg.topic)] == time.time():
                    return
                else:
                    last_message[str(msg.topic)] = time.time() #ajouter +- 2s
                    writer.writerow(mydict)
            else:
                last_message[str(msg.topic)] = time.time()
                writer.writerow(mydict)
            

        client.subscribe(TOPIC)
        client.on_message = on_message


    def on_connect(client, userdata, flags, rc, *i):
        if rc == 0 and client.is_connected():
            print("Connected to MQTT Broker!")
            client.subscribe(TOPIC)
        else:
            print(f'Failed to connect, return code {rc}')




    def on_disconnect(client, userdata, rc):
        logging.info("Disconnected with result code: %s", rc)
        reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
        while reconnect_count < MAX_RECONNECT_COUNT:
            logging.info("Reconnecting in %d seconds...", reconnect_delay)
            time.sleep(reconnect_delay)


            try:
                client.reconnect()
                logging.info("Reconnected successfully!")
                return
            except Exception as err:
                logging.error("%s. Reconnect failed. Retrying...", err)


            reconnect_delay *= RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
            reconnect_count += 1
        logging.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)

    fields = ["topic","time", "Valeur 1: Temperature_1","Valeur 2: Humidity_2","Valeur 3: CO2 PPM_3","Valeur 4: VOC PPB_4","Valeur 5: Luminosité lux_5","linkquality"]


    writer = csv.DictWriter(csvfile, fieldnames=fields)
    writer.writeheader()
    client = mqtt_client.Client(client_id=CLIENT_ID, callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.connect(BROKER, PORT)
    subscribe(client)
    client.loop_forever()
