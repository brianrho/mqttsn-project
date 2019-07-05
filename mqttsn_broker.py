from typing import List, Callable

from structures import *
from mqttsn_transport import MQTTSNTransport
from mqtt_client import MQTTClient
from enum import IntEnum, unique
import time
import collections


class MQTTSNInstance:
    sub_topics: List[MQTTSNSubTopic] = [MQTTSNSubTopic] * MQTTSN_MAX_NUM_TOPICS
    pub_topics: List[MQTTSNPubTopic] = [MQTTSNPubTopic] * MQTTSN_MAX_NUM_TOPICS

    cid: bytes = b''
    flags: MQTTSNFlags

    address: bytes = b''
    msg_inflight: bytes = None
    unicast_timer: float = 0
    unicast_counter: float = 0

    keepalive_duration: int = MQTTSN_DEFAULT_KEEPALIVE
    last_in: float

    def __init__(self):
        pass

    def add_sub_topic(self, name: bytes, flags: MQTTSNFlags):
        for i in range(MQTTSN_MAX_NUM_TOPICS):
            if self.sub_topics[i].tid == 0:
                self.sub_topics[i].name = name
                self.sub_topics[i].tid = i + 1
                self.sub_topics[i].flags = flags
                return self.sub_topics[i].tid
        else:
            return 0

    def add_pub_topic(self, name: bytes):
        for i in range(MQTTSN_MAX_NUM_TOPICS):
            if self.pub_topics[i].tid == 0:
                self.pub_topics[i].name = name
                self.pub_topics[i].tid = i + 1
                return self.pub_topics[i].tid
        else:
            return 0

    def delete_sub_topic(self, name: bytes):
        for i in range(MQTTSN_MAX_NUM_TOPICS):
            if self.sub_topics[i].name == name:
                self.sub_topics[i].name = ''
                self.sub_topics[i].tid = 0
                self.sub_topics[i].flags = None
                return True
        else:
            return False

    def delete_pub_topic(self, name: bytes):
        for i in range(MQTTSN_MAX_NUM_TOPICS):
            if self.pub_topics[i].name == name:
                self.pub_topics[i].name = b''
                self.pub_topics[i].tid = 0
                return True
        else:
            return False


@unique
class MQTTSNState(IntEnum):
    ACTIVE = 0
    LOST = 1
    ASLEEP = 2
    AWAKE = 3
    DISCONNECTED = 4

    CONNECTING = 5
    SEARCHING = 6


class MQTTSNGWInfo:
    def __init__(self, gwid: int = 0, gwaddr: bytes = b''):
        self.gwid = gwid
        self.gwaddr = gwaddr
        self.available = True


class MQTTSNBroker:
    # list of topics
    pub_topics: List[MQTTSNPubTopic]
    sub_topics: List[MQTTSNSubTopic]

    # list of clients
    clients: List[MQTTSNInstance] = [MQTTSNInstance()] * MQTTSN_MAX_NUM_CLIENTS

    # handle incoming messages
    msg_handlers: List[Callable[[bytes, bytes], None]] = [None] * len(MQTTSN_MSG_TYPES)

    # handle client states
    state_handlers: List[Callable[[], None]] = [None] * len(MQTTSNState.__members__)

    def __init__(self, gw_id: int, mqttc: MQTTClient, transport: MQTTSNTransport):
        self.gw_id = gw_id
        self.transport = transport
        self.state = MQTTSNState.DISCONNECTED

        self.mqttc = mqttc
        self.mqttc.register_handlers(self._handle_mqtt_conn, self._handle_mqtt_publish)

        # flag for keeping track of MQTT broker connection
        self.connected = False

        # for messages that expect a reply
        self.curr_msg_id = 0

        # queue for msgs yet to be published to MQTT-SN clients
        self.pub_queue = collections.deque(maxlen=MQTTSN_MAX_QUEUED_PUBLISH)

        self._assign_handlers()

    def _assign_handlers(self):
        self.msg_handlers[SEARCHGW] = self._handle_searchgw
        self.msg_handlers[CONNECT] = self._handle_connect
        self.msg_handlers[REGISTER] = self._handle_register
        self.msg_handlers[SUBSCRIBE] = self._handle_subscribe
        self.msg_handlers[UNSUBSCRIBE] = self._handle_unsubscribe
        self.msg_handlers[PUBLISH] = self._handle_publish
        self.msg_handlers[PINGREQ] = self._handle_pingreq

        self.state_handlers[MQTTSNState.ACTIVE] = self._active_handler

    def loop(self):
        # make sure to handle msgs first
        # so that the updated states get selected
        self._handle_messages()

        # run state handler
        if self.state_handlers[self.state] is not None:
            self.state_handlers[self.state]()

        return self.state == MQTTSNState.ACTIVE

    def _handle_messages(self):
        while True:
            # try to read something, return if theres nothing
            pkt, from_addr = self.transport.read_packet()
            if not pkt:
                return

            # parse the header so we can get the msg type
            header = MQTTSNHeader()
            rlen = header.unpack(pkt)

            # if it failed somehow
            if not rlen:
                continue

            # check that a handler exists
            idx = header.msg_type
            if idx >= len(self.msg_handlers) or self.msg_handlers[idx] is None:
                continue

            # call the msg handler
            self.msg_handlers[idx](pkt[rlen:], from_addr)

    def _handle_searchgw(self, pkt, from_addr):
        msg = MQTTSNMessageSearchGW()
        if not msg.unpack(pkt):
            return

        reply = MQTTSNMessageGWInfo()
        reply.id = self.gw_id
        raw = reply.pack()
        self.transport.broadcast(raw)
        return

    def _handle_connect(self, pkt, from_addr):
        msg = MQTTSNMessageConnect()
        if not msg.unpack(pkt) or not msg.client_id:
            return

        reply = MQTTSNMessageConnack()
        reply.return_code = MQTTSN_RC_ACCEPTED

        for clnt in self.clients:
            if not clnt.cid:
                clnt.cid = msg.client_id
                clnt.address = from_addr
                clnt.keepalive_duration = msg.duration
                clnt.flags = msg.flags
                clnt.msg_inflight = None
                clnt.last_in = time.time()
                break
        else:
            # no space left for new clients
            reply.return_code = MQTTSN_RC_CONGESTION

        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

    def _get_instance(self, addr):
        for clnt in self.clients:
            if clnt.address == addr:
                return clnt
        else:
            return None

    def _handle_register(self, pkt, from_addr):
        clnt = self._get_instance(from_addr)
        if not clnt:
            return

        clnt.last_in = time.time()

        # unpack the msg
        msg = MQTTSNMessageRegister()
        if not msg.unpack(pkt) or msg.topic_id != 0x0000:
            return

        # construct regack response
        reply = MQTTSNMessageRegack()
        reply.msg_id = msg.msg_id

        # try to add the topic to the instance
        tid = clnt.add_pub_topic(msg.topic_name)
        if tid == 0:
            reply.return_code = MQTTSN_RC_CONGESTION
        else:
            reply.topic_id = tid

        # now send our reply
        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

    def _handle_publish(self, pkt, from_addr):
        # wont check the gw address
        # have faith that only our connected gw will send us msgs
        if not self.connected:
            return

        # now unpack the message, only QoS 0 for now
        msg = MQTTSNMessagePublish()
        if not msg.unpack(pkt) or msg.msg_id != 0x0000:
            return

        # get the topic name
        for t in self.sub_topics:
            if t.tid == msg.topic_id:
                topic = t.name
                break
        else:
            return

        # call user handler
        self.publish_cb(topic, msg.data, msg.flags)

    def _handle_subscribe(self, pkt, from_addr):
        clnt = self._get_instance(from_addr)
        if not clnt:
            return

        clnt.last_in = time.time()

        # unpack the msg
        msg = MQTTSNMessageSubscribe()
        if not msg.unpack(pkt):
            return

        # construct suback response
        reply = MQTTSNMessageSuback()
        reply.msg_id = msg.msg_id

        # try to add the topic to the instance
        tid = clnt.add_sub_topic(msg.topic_id_name, msg.flags)
        if tid == 0:
            reply.return_code = MQTTSN_RC_CONGESTION
        else:
            reply.topic_id = tid

        # now send our reply
        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

        # here we issue our subscribe to MQTT clnt

    def _handle_unsubscribe(self, pkt, from_addr):
        clnt = self._get_instance(from_addr)
        if not clnt:
            return

        clnt.last_in = time.time()

        # unpack the msg
        msg = MQTTSNMessageUnsubscribe()
        if not msg.unpack(pkt):
            return

        # construct unsuback response
        reply = MQTTSNMessageUnsuback()
        reply.msg_id = msg.msg_id

        # try to remove the topic from the instance
        rc = clnt.delete_sub_topic(msg.topic_id_name)
        if not rc:
            return

        # now send our reply
        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

        # here we issue our unsubscribe to MQTT clnt

    def _handle_pingreq(self, pkt, from_addr):
        clnt = self._get_instance(from_addr)
        if not clnt:
            return

        clnt.last_in = time.time()

        msg = MQTTSNMessagePingreq()
        if not msg.unpack(pkt):
            return

        # now send our reply
        reply = MQTTSNMessagePingresp()
        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

    def _handle_mqtt_conn(self, conn_state: bool):
        # now we know we're no longer connected to MQTT broker
        if not conn_state:
            self.connected = False
            return

        if self.connected:
            return

        # re-subscribe to all topics
        self.connected = True
        for clnt in self.clients:
            for topic in clnt.sub_topics:
                self.mqttc.subscribe(topic.name, topic.flags.qos)

    def _handle_mqtt_publish(self):
        pass

    def _active_handler(self):
        # check for any expected replies
        # not very useful now, till we have QoS > 0 and wildcards
        for clnt in self.clients:
            if clnt.msg_inflight:
                if time.time() - clnt.unicast_timer < MQTTSN_T_RETRY:
                    continue

                clnt.unicast_timer = time.time()
                clnt.unicast_counter += 1
                if clnt.unicast_counter > MQTTSN_N_RETRY:
                    clnt.cid = b''
                    clnt.address = b''

        while self.pub_queue:
            pkt = self.pub_queue.popleft()

            # now unpack the message, only QoS 0 for now
            msg = MQTTSNMessagePublish()
            if not msg.unpack(pkt) or msg.msg_id != 0x0000:
                return

            # have to somehow store the sender so we can know the mapping

        # use this for now, so we can change fraction later
        duration = self.keep_alive_duration

        curr_time = time.time()
        if curr_time >= self.last_out + duration or curr_time >= self.last_in + duration:
            if not self.pingresp_pending:
                self.ping()
                self.last_out = time.time()
                self.pingresp_pending = True
            elif curr_time - self.pingreq_timer >= MQTTSN_T_RETRY:
                # just keep trying till we hit the keepalive limit
                if curr_time >= self.last_in + self.keep_alive_duration * 1.5:
                    self.state = MQTTSNState.LOST
                    self.connected = False
                    self.pingresp_pending = False
                else:
                    self.ping()
                    self.pingreq_timer = time.time()
                    self.last_out = time.time()
