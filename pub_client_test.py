from mqttsn_transport_udp import MQTTSNTransportUDP
from mqttsn_client import MQTTSNClient, MQTTSNState, MQTTSNGWInfo, MQTTSNPubTopic, MQTTSNSubTopic
from mqttsn_messages import MQTTSNFlags
import time

# list of gateways
gateways = [MQTTSNGWInfo(1, b'\x01')]

# setup transport info
port = 20000
transport = MQTTSNTransportUDP(port, b'\x03')

print("Starting client.")

# create client and connect
clnt = MQTTSNClient(b'PubClient', transport)
clnt.add_gateways(gateways)
clnt.connect(gwid=1)

# wait till we're connected
while not clnt.is_connected() or clnt.state == MQTTSNState.CONNECTING:
    time.sleep(0.05)

# pub and sub topics
sub_topics = [MQTTSNSubTopic(b'state')]
pub_topics = [MQTTSNPubTopic(b'led')]

# just to hold state, must be a list because python
led_state = [b'\x00']


def message_callback(topic: bytes, data: bytes, flags: MQTTSNFlags):
    out = 'Topic: {}, Data: {}, Flags: {}, '.format(topic, data, flags.union)
    print(out, end='')

    led_state[0] = data
    print('State: ', led_state[0])


# register callback for incoming publish
clnt.on_message(message_callback)


def init_tasks():
    if not clnt.register_topics(pub_topics):
        return False

    if not clnt.subscribe_topics(sub_topics):
        return False

    return True


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
            clnt.publish(b'led', b'\x00' if led_state[0][0] else b'\x01')
            last_publish = time.time()
    except KeyboardInterrupt:
        break
