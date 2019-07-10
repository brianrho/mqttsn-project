from mqtt_client_paho import MQTTClientPaho
from mqttsn_gateway import MQTTSNGateway
from mqttsn_transport_udp import MQTTSNTransportUDP
import time
import logging
import sys


logging.basicConfig(stream=sys.stdout, format='[+]%(message)s', level=logging.DEBUG)

# create MQTT client
mqttc = MQTTClientPaho('broker.hivemq.com', 1883)

# setup transport info
port = 20000
transport = MQTTSNTransportUDP(port, b'\x01')

# now create gateway, supply the client and transport
gateway = MQTTSNGateway(1, mqttc, transport)

# connect to MQTT broker
mqttc.connect()

# enter main loop
while True:
    try:
        time.sleep(0.05)
        gateway.loop()
        mqttc.loop()
    except KeyboardInterrupt:
        break
