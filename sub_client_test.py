from mqttsn_transport_udp import MQTTSNTransportUDP
from mqttsn_client import MQTTSNClient, MQTTSNState, MQTTSNGWInfo, MQTTSNPubTopic, MQTTSNSubTopic
from mqttsn_messages import MQTTSNFlags
import time

# list of gateways
gateways = [MQTTSNGWInfo(1, b'\x01')]

# setup transport info
port = 20000
transport = MQTTSNTransportUDP(port, b'\x02')

print("Starting client.")

# create client and connect
clnt = MQTTSNClient(b'SubClient', transport)
clnt.add_gateways(gateways)
clnt.connect(gwid=1)

# wait till we're connected
while not clnt.is_connected() or clnt.state == MQTTSNState.CONNECTING:
    time.sleep(0.05)

# pub and sub topics
sub_topics = [MQTTSNSubTopic(b'led')]
pub_topics = [MQTTSNPubTopic(b'state')]

# just to hold state, must be a list because python
led_state = [b'\x00']


def message_callback(topic: bytes, data: bytes, flags: MQTTSNFlags):
    out = 'Topic: {}, Data: {}, Flags: {}'.format(topic, data, flags.union)
    print(out)

    # set the led state and publish
    led_state[0] = data
    clnt.publish(b'state', led_state[0])


# register callback for incoming publish
clnt.on_message(message_callback)


def init_tasks():
    if not clnt.register_topics(pub_topics):
        return False

    if not clnt.subscribe_topics(sub_topics):
        return False

    return True


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
    except KeyboardInterrupt:
        break
