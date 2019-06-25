from mqttsn_transport_udp import MQTTSNTransportUDP
from mqttsn_client import MQTTSNClient, MQTTSNState
import time

gw_addr = b'\x01'
port = 20000
transport = MQTTSNTransportUDP(port, b'\x02')
print("Starting client.")

clnt = MQTTSNClient('TestClient', transport)
clnt.add_gateway(0x1, b'\x01')
clnt.connect(gwid=0x1)
while not clnt.is_connected() or clnt.state == MQTTSNState.CONNECT_IN_PROGRESS:
    pass


in_topics = [b'/button']
out_topics = [b'/led']


def init_tasks():
    for topic in in_topics:
        clnt.subscribe(topic)
        while clnt.transaction_pending():
            yield

    for topic in out_topics:
        clnt.register(topic)
        while clnt.transaction_pending():
            yield


last_publish = time.time()
while True:
    if clnt.state in (MQTTSNState.DISCONNECTED, MQTTSNState.LOST):
        print("Gateway connection lost.")

    if time.time() - last_publish > 5:
        clnt.publish('/led', b'\x01')
        last_publish = time.time()

    clnt.loop()
