from mqtt_client_paho import MQTTClientPaho
from mqttsn_broker import MQTTSNBroker
from mqttsn_transport_udp import MQTTSNTransportUDP
import time

# create MQTT client
mqttc = MQTTClientPaho('iot.eclipse.org', 1883)

# setup transport info
port = 20000
transport = MQTTSNTransportUDP(port, b'\x01')

# now create gateway, supply the client and transport
gateway = MQTTSNBroker(1, mqttc, transport)

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
