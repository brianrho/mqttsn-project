from mqtt_client import MQTTClient
import paho.mqtt.client as mqtt
import socket
import time
from mqttsn_messages import MQTTSNFlags


class MQTTClientPaho(MQTTClient):
    def __init__(self, server, port, cid='', username='', password=''):
        self.server = server
        self.port = port
        self.cid = cid
        self.username = username
        self.password = password

        self.client = mqtt.Client(cid)
        self.client.username_pw_set(self.username, self.password)

        # callback registered by mqttsn broker to know of conn/disconns
        self.broker_conn_cb = None
        self.broker_msg_cb = None

        # register internal callback for handling mosquitto conn/disconn/msgs
        self.client.on_connect = self.connect_cb
        self.client.on_disconnect = self.disconnect_cb
        self.client.on_message = self.message_cb

        self.last_connect = 0

    def connect(self):
        self.client.connect(self.server, self.port)

    # register handlers for connection updates
    # and incoming PUBLISH msgs
    def register_handlers(self, conn_disconn_cb, msg_cb):
        self.broker_conn_cb = conn_disconn_cb
        self.broker_msg_cb = msg_cb

    def publish(self, topic, data, qos=0, retain=False):
        topic = topic.decode()
        self.client.publish(topic, data, qos, retain)

    def subscribe(self, topic, qos=0):
        topic = topic.decode()
        self.client.subscribe(topic, qos)

    def unsubscribe(self, topic):
        self.client.unsubscribe(topic)

    # called whenever we get a PUBLISH message
    def message_cb(self, client, userdata, message: mqtt.MQTTMessage):
        flags = MQTTSNFlags()
        flags.retain = message.retain
        flags.qos = message.qos
        self.broker_msg_cb(message.topic.encode(), message.payload, flags)

    # called whenever we have a conn/disconn event
    def connect_cb(self, client, userdata, flags, rc):
        if rc == 0:
            self.broker_conn_cb(True)
        else:
            self.broker_conn_cb(False)

    def disconnect_cb(self, client, userdata, rc):
        self.broker_conn_cb(False)

    def loop(self):
        rc = self.client.loop()
        if rc != mqtt.MQTT_ERR_SUCCESS:
            try:
                if time.time() > self.last_connect + 1:
                    self.client.reconnect()
                    self.last_connect = time.time()
            except socket.error:
                self.last_connect = time.time()
