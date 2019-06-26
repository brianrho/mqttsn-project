from typing import List, Callable

from structures import *
from mqttsn_transport import MQTTSNTransport
from enum import IntEnum, unique
import time
import random


class MQTTSNInstance:
    sub_topics: List[MQTTSNSubTopic] = [MQTTSNSubTopic] * MQTTSN_MAX_NUM_TOPICS
    pub_topics: List[MQTTSNPubTopic] = [MQTTSNPubTopic] * MQTTSN_MAX_NUM_TOPICS

    cid: bytes = b''
    flags: MQTTSNFlags

    address: bytes = b''
    msg_inflight: bytes = None
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

    def __init__(self, gw_id: int, transport: MQTTSNTransport):
        self.gw_id = gw_id
        self.transport = transport
        self.state = MQTTSNState.DISCONNECTED

        # store the current gw
        self.curr_gateway = None
        self.num_gateways = 0

        self.connected = False

        # store unicast msg so we can use it when retrying
        self.msg_inflight = None
        self.unicast_timer = time.time()
        self.unicast_counter = 0

        self.keep_alive_duration = MQTTSN_DEFAULT_KEEPALIVE

        # keep track of time since unicast messages that expect a reply
        self.last_out = 0
        self.last_in = 0

        # tracking pings
        self.pingresp_pending = False
        self.pingreq_timer = 0
        self.pingreq_counter = 0

        # for keeping track of discovery
        self.searchgw_started = 0
        self.searchgw_interval = 0
        self.searchgw_pending = False

        # for messages that expect a reply
        self.curr_msg_id = 0

        # for counting number of topics
        self.sub_topics_cnt = 0
        self.pub_topics_cnt = 0

        self.num_topics = 0
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
        self.state_handlers[MQTTSNState.CONNECTING] = self._connecting_handler
        self.state_handlers[MQTTSNState.LOST] = self._lost_handler
        self.state_handlers[MQTTSNState.DISCONNECTED] = self._disconnected_handler

    def loop(self):
        # make sure to handle msgs first
        # so that the updated states get selected
        self._handle_messages()

        # check up on any messages awaiting a reply
        self._inflight_handler()

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

            # get the type
            header = MQTTSNHeader()
            rlen = header.unpack(pkt)

            # if its somehow empty
            if not rlen:
                continue

            # check that a handler exists
            idx = header.msg_type
            if idx >= len(self.msg_handlers) or self.msg_handlers[idx] is None:
                continue

            # call the msg handler
            self.msg_handlers[idx](pkt[rlen:], from_addr)

    def _inflight_handler(self):
        if self.msg_inflight is None:
            return

        # check if we've timed out after Nretry * Tretry secs
        if time.time() - self.unicast_timer >= MQTTSN_T_RETRY:
            self.unicast_timer = time.time()
            self.unicast_counter += 1
            if self.unicast_counter > MQTTSN_N_RETRY:
                self.connected = False
                self.msg_inflight = None
                self.state = MQTTSNState.LOST

                # Mark the gateway as unavailable
                for i in range(len(self.gateways)):
                    if self.gateways[i].gwid == self.curr_gateway.gwid:
                        self.gateways[i].available = False
                        break

                return False

            # resend msg
            self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)
            return False

    def register_topics(self, topics: List[MQTTSNPubTopic]):
        self.pub_topics = topics
        self.pub_topics_cnt = len(topics)

        # if we're not connected or theres a pending reply
        if not self.is_connected() or self.msg_inflight:
            return False

        # register any unregistered topics
        for t in self.pub_topics:
            if t.tid == 0:
                self._register(t)
                return False
        else:
            return True

    def _register(self, topic: MQTTSNPubTopic):
        msg = MQTTSNMessageRegister()
        msg.topic_name = topic
        msg.topic_id = 0
        # 0 is reserved for message IDs
        self.curr_msg_id = 1 if self.curr_msg_id == 0 else self.curr_msg_id
        msg.msg_id = self.curr_msg_id

        self.msg_inflight = msg.pack()
        self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)

        self.last_out = time.time()
        self.unicast_timer = time.time()
        self.unicast_counter = 0

        # always a 16-bit value
        self.curr_msg_id = (self.curr_msg_id + 1) & 0xFFFF

    def publish(self, topic, data, flags=None):
        # if we're not connected
        if not self.is_connected():
            return False

        # get the topic id
        msg = MQTTSNMessagePublish()
        for t in self.pub_topics:
            if t.name == topic:
                msg.topic_id = t.tid
                break
        else:
            return False

        # msgid = 0 for qos 0
        if flags.qos in (1, 2):
            # 0 is reserved
            self.curr_msg_id = 1 if self.curr_msg_id == 0 else self.curr_msg_id
            msg.msg_id = self.curr_msg_id

        msg.flags = flags
        msg.data = data
        raw = msg.pack()
        self.transport.write_packet(raw, self.curr_gateway.gwaddr)

        # need to note last_out for QoS 1 publish

        # always a 16-bit value
        self.curr_msg_id = (self.curr_msg_id + 1) & 0xFFFF
        return True

    def subscribe_topics(self, topics: List[MQTTSNSubTopic]):
        self.sub_topics = topics
        self.sub_topics_cnt = len(topics)

        # if we're not connected or theres a pending reply
        if not self.is_connected() or self.msg_inflight:
            return False

        # register any unregistered topics
        for t in self.sub_topics:
            if t.tid == 0:
                self._subscribe(t)
                return False
        else:
            return True

    def _subscribe(self, topic: MQTTSNSubTopic):
        msg = MQTTSNMessageSubscribe()
        msg.topic_id_name = topic.name

        # 0 is reserved for message IDs
        self.curr_msg_id = 1 if self.curr_msg_id == 0 else self.curr_msg_id
        msg.msg_id = self.curr_msg_id
        msg.flags = topic.flags

        self.msg_inflight = msg.pack()
        self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)

        self.last_out = time.time()
        self.unicast_timer = time.time()
        self.unicast_counter = 0

        # always a 16-bit value
        self.curr_msg_id = (self.curr_msg_id + 1) & 0xFFFF

    def unsubscribe(self, topic: str, flags=None):
        # if we're not connected or there's a pending transaction
        if not self.is_connected() or self.msg_inflight:
            return False

        msg = MQTTSNMessageUnsubscribe()

        # check our list of subs for this topic
        for t in self.sub_topics:
            if t.name == topic:
                msg.topic_id_name = topic
                break
        else:
            return False

        # 0 is reserved
        self.curr_msg_id = 1 if self.curr_msg_id == 0 else self.curr_msg_id
        msg.msg_id = self.curr_msg_id
        msg.flags = flags

        self.msg_inflight = msg.pack()
        self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)

        self.last_out = time.time()
        self.unicast_timer = time.time()
        self.unicast_counter = 0

        # always a 16-bit value
        self.curr_msg_id = (self.curr_msg_id + 1) & 0xFFFF

        # TODO: Consider removing the topic here since unsuback doesnt really matter

    def ping(self):
        if not self.connected or self.pingresp_pending:
            return

        msg = MQTTSNMessagePingreq()
        raw = msg.pack()
        self.transport.write_packet(raw, self.curr_gateway.gwaddr)

        self.last_out = time.time()
        self.pingreq_timer = time.time()

    def transaction_pending(self):
        if self.msg_inflight is None:
            return False

        self.loop()
        return self.msg_inflight is not None

    def is_connected(self):
        if self.connected:
            return True

        return self.loop()

    def disconnect(self):
        if not self.connected:
            return

        msg = MQTTSNMessageDisconnect()
        raw = msg.pack()
        self.transport.write_packet(raw, self.curr_gateway.gwaddr)
        self.connected = False
        self.state = MQTTSNState.DISCONNECTED

    def on_publish(self, callback):
        self.publish_cb = callback

    def _handle_searchgw(self, pkt, from_addr):
        msg = MQTTSNMessageSearchGW()
        if not msg.unpack(pkt):
            return

        reply = MQTTSNMessageGWInfo()
        reply.id = self.gw_id
        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)
        return

    def _handle_connect(self, pkt, from_addr):
        if self.msg_inflight is None:
            return

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
        if not self.curr_gateway or not self.connected:
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

    def _connecting_handler(self):
        if self.connected:
            self.state = MQTTSNState.ACTIVE
            return

    def _lost_handler(self):
        if self.connected:
            self.state = MQTTSNState.ACTIVE
            return

        # try to connect
        self.connect(self.curr_gateway.gwid)

    def _disconnected_handler(self):
        if self.connected:
            self.state = MQTTSNState.ACTIVE
            return

    def _active_handler(self):
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
