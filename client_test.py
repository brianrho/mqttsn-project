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
clnt = MQTTSNClient('TestClient', transport)
clnt.add_gateways(gateways)
clnt.connect(gwid=1)

# wait till we're connected
while not clnt.is_connected() or clnt.state == MQTTSNState.CONNECT_IN_PROGRESS:
    pass

# pub and sub topics
sub_topics = [MQTTSNSubTopic(b'button')]
pub_topics = [MQTTSNPubTopic(b'led')]


def init_tasks():
    if not clnt.register_topics(pub_topics):
        return False

    if not clnt.subscribe_topics(sub_topics):
        return False

    return True


led_state = 1
last_publish = time.time()

while True:
    clnt.loop()

    if clnt.state in (MQTTSNState.DISCONNECTED, MQTTSNState.LOST):
        print("Gateway connection lost.")

    # check if all the pubs and subs are done
    if not init_tasks():
        continue

    # toggle led state and publish every 5 secs
    if time.time() - last_publish > 5:
        led_state ^= 1
        clnt.publish('/led', led_state.to_bytes(1, 'big'))
        last_publish = time.time()
