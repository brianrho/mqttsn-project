from mqttsn_transport_udp import MQTTSNTransportUDP
from mqttsn_client import MQTTSNClient, MQTTSNState, MQTTSNGWInfo
from structures import MQTTSNPubTopic, MQTTSNSubTopic
import time

# list of gateways
gateways = [MQTTSNGWInfo(1, b'\x01')]

# setup transport info
port = 20000
transport = MQTTSNTransportUDP(port, b'\x02')

print("Starting client.")

# create client and connect
clnt = MQTTSNClient(b'TestClient', transport)
clnt.add_gateways(gateways)
clnt.connect(gwid=1)

# wait till we're connected
while not clnt.is_connected() or clnt.state == MQTTSNState.CONNECTING:
    time.sleep(0.05)

# pub and sub topics
sub_topics = [MQTTSNSubTopic(b'button')]
pub_topics = [MQTTSNPubTopic(b'led')]


def init_tasks():
    if not clnt.register_topics(pub_topics):
        return False

    if not clnt.subscribe_topics(sub_topics):
        return False

    return True


led_state = bytearray([0])
last_publish = time.time()

print('Entering client loop.')
while True:
    try:
        time.sleep(0.05)
        clnt.loop()

        if clnt.state in (MQTTSNState.DISCONNECTED, MQTTSNState.LOST):
            print("Gateway connection lost.")

        # check if all the pubs and subs are done
        if not init_tasks():
            continue

        # toggle led state and publish every 5 secs
        if time.time() - last_publish > 5:
            led_state[0] ^= 1
            clnt.publish(b'led', led_state)
            last_publish = time.time()
    except KeyboardInterrupt:
        break
