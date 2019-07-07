from typing import List, Callable

from structures import *
from mqttsn_transport import MQTTSNTransport
from mqtt_client import MQTTClient
from enum import IntEnum, unique
import time
import collections


class MQTTSNInstancePubTopic:
    def __init__(self, tid=0):
        self.tid = tid


class MQTTSNInstanceSubTopic:
    def __init__(self, tid=0, flags=None):
        self.tid = tid
        self.flags = flags if flags else MQTTSNFlags()


@unique
class MQTTSNInstanceStatus(IntEnum):
    ACTIVE = 1
    DISCONNECTED = 3
    LOST = 4


class MQTTSNInstance:
    transport: MQTTSNTransport

    sub_topics: List[MQTTSNInstanceSubTopic] = [MQTTSNInstanceSubTopic] * MQTTSN_MAX_INSTANCE_TOPICS
    pub_topics: List[MQTTSNInstancePubTopic] = [MQTTSNInstancePubTopic] * MQTTSN_MAX_INSTANCE_TOPICS

    cid: bytes = b''
    flags: MQTTSNFlags

    address: bytes = b''
    msg_inflight: bytes = b''
    unicast_timer: float = 0
    unicast_counter: float = 0

    keepalive_duration: int = MQTTSN_DEFAULT_KEEPALIVE
    last_in: float = 0
    status: MQTTSNInstanceStatus = MQTTSNInstanceStatus.DISCONNECTED

    def __init__(self):
        pass

    # insert new client's details
    def register(self, cid, address, duration, flags):
        self.cid = cid
        self.address = address
        self.keepalive_duration = duration
        self.flags = flags

        for topic in self.sub_topics:
            topic.tid = 0

        for topic in self.pub_topics:
            topic.tid = 0

        self.msg_inflight = b''
        self.status = MQTTSNInstanceStatus.ACTIVE
        self.mark_time()

    def deregister(self):
        self.cid = b''
        self.address = b''
        self.status = MQTTSNInstanceStatus.DISCONNECTED

    def register_transport(self, transport):
        self.transport = transport

    def __bool__(self):
        return bool(self.cid)

    def add_sub_topic(self, tid, flags: MQTTSNFlags):
        # check if we're already subbed
        # and only update the flags if so
        for i in range(MQTTSN_MAX_INSTANCE_TOPICS):
            if self.sub_topics[i].tid == tid:
                self.sub_topics[i].flags = flags
                return True

        # else add the new topic to our list
        for i in range(MQTTSN_MAX_INSTANCE_TOPICS):
            if self.sub_topics[i].tid == 0:
                self.sub_topics[i].tid = tid
                self.sub_topics[i].flags = flags
                return True

        # no more space
        return False

    def add_pub_topic(self, tid):
        # check if we're already registered
        for i in range(MQTTSN_MAX_INSTANCE_TOPICS):
            if self.pub_topics[i].tid == tid:
                return True

        # else add the new topic to our list
        for i in range(MQTTSN_MAX_INSTANCE_TOPICS):
            if self.pub_topics[i].tid == 0:
                self.pub_topics[i].tid = tid
                return True

        # no more space
        return False

    def delete_sub_topic(self, tid):
        # check if we've got it and delete it
        for i in range(MQTTSN_MAX_INSTANCE_TOPICS):
            if self.sub_topics[i].tid == tid:
                self.sub_topics[i].tid = 0
                self.sub_topics[i].flags = None
                return True

        # return true anyways so we can re-send unsuback if it got lost
        return False

    # no real use for this yet
    def delete_pub_topic(self, tid):
        for i in range(MQTTSN_MAX_INSTANCE_TOPICS):
            if self.pub_topics[i].tid == tid:
                self.pub_topics[i].tid = 0
                return True
        else:
            return False

    # check if we're subbed to a topic
    def is_subbed(self, tid):
        for topic in self.sub_topics:
            if topic.tid == tid:
                return True

        return False

    def check_status(self):
        # check last time we got a control packet
        if time.time() - self.last_in > self.keepalive_duration * 1.5:
            self.status = MQTTSNInstanceStatus.LOST
            return self.status

        # check if there are any outstanding msgs
        if not self.msg_inflight:
            return self.status

        # check if retry timer is up
        if time.time() - self.unicast_timer < MQTTSN_T_RETRY:
            return self.status

        self.unicast_timer = time.time()
        self.unicast_counter += 1

        # check if retry counter is up
        if self.unicast_counter > MQTTSN_N_RETRY:
            self.status = MQTTSNInstanceStatus.LOST
            return self.status

        # resend the msg if not
        self.transport.write_packet(self.msg_inflight, self.address)
        return self.status

    def mark_time(self):
        self.last_in = time.time()


# for holding the broker's list of topic ID mappings
class MQTTSNBrokerTopic:
    def __init__(self, name='', tid=0, ttype=0):
        self.name = name
        self.tid = tid
        self.type = ttype


class MQTTSNBroker:
    # list of topics
    topics: List[MQTTSNBrokerTopic] = [MQTTSNBrokerTopic()] * MQTTSN_MAX_BROKER_TOPICS

    # list of clients
    clients: List[MQTTSNInstance] = [MQTTSNInstance()] * MQTTSN_MAX_NUM_CLIENTS

    # handle incoming messages
    msg_handlers: List[Callable[[bytes, bytes], None]] = [None] * len(MQTTSN_MSG_TYPES)

    def __init__(self, gw_id: int, mqttc: MQTTClient, transport: MQTTSNTransport):
        self.gw_id = gw_id
        self.transport = transport

        # MQTT client handles, also register the relevant handlers
        self.mqttc = mqttc
        if self.mqttc:
            self.mqttc.register_handlers(self._handle_mqtt_conn, self._handle_mqtt_publish)

        # flag for keeping track of MQTT client connection
        self.connected = False

        # for messages that expect a reply
        self.curr_msg_id = 0

        # queue for msgs yet to be published to MQTT-SN clients
        self.pub_queue = collections.deque(maxlen=MQTTSN_MAX_QUEUED_PUBLISH)

        # handlers for MQTT-SN msgs we get from clients
        self._assign_msg_handlers()

    def _assign_msg_handlers(self):
        self.msg_handlers[SEARCHGW] = self._handle_searchgw
        self.msg_handlers[CONNECT] = self._handle_connect
        self.msg_handlers[REGISTER] = self._handle_register
        self.msg_handlers[SUBSCRIBE] = self._handle_subscribe
        self.msg_handlers[UNSUBSCRIBE] = self._handle_unsubscribe
        self.msg_handlers[PUBLISH] = self._handle_publish
        self.msg_handlers[PINGREQ] = self._handle_pingreq

    # main gateway loop
    def loop(self):
        self._handle_messages()

        # check keepalive and inflight messages
        for clnt in self.clients:
            if clnt and clnt.check_status() == MQTTSNInstanceStatus.LOST:
                clnt.deregister()

        # now distribute any pending publish msgs
        # from the queue
        while self.pub_queue:
            pkt = self.pub_queue.popleft()

            # parse the header so we can get the msg type
            header = MQTTSNHeader()
            rlen = header.unpack(pkt)

            # if it failed somehow
            if not rlen or header.msg_type != PUBLISH:
                continue

            # now unpack the message, only QoS 0 for now
            msg = MQTTSNMessagePublish()
            if not msg.unpack(pkt) or msg.msg_id != 0x0000:
                return

            tid = msg.topic_id
            for clnt in self.clients:
                if clnt.is_subbed(tid):
                    self.transport.write_packet(pkt, clnt.address)

        # just to return something useful
        return self.connected

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

            print("Got something: ", pkt)

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

        # prepare connack
        reply = MQTTSNMessageConnack()
        reply.return_code = MQTTSN_RC_ACCEPTED

        # try to check if the client is already connected
        clnt = self._get_instance(from_addr)
        if clnt:
            # if we do have an existing session, overwrite it
            clnt.register(msg.client_id, from_addr, msg.duration, msg.flags)
            clnt.register_transport(self.transport)
        else:
            # else create a new instance
            for clnt in self.clients:
                if not clnt:
                    clnt.register(msg.client_id, from_addr, msg.duration, msg.flags)
                    clnt.register_transport(self.transport)
                    break
            else:
                # no space left for new clients
                reply.return_code = MQTTSN_RC_CONGESTION

        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

    def _get_topic_id(self, name):
        # check if we already have that topic
        for idx in range(MQTTSN_MAX_BROKER_TOPICS):
            if self.topics[idx].name == name:
                return self.topics[idx].tid

        # else, add it
        for idx in range(MQTTSN_MAX_BROKER_TOPICS):
            if self.topics[idx].name == '':
                self.topics[idx].name = name
                self.topics[idx].tid = idx + 1
                return self.topics[idx].tid

        return 0

    def _get_topic_name(self, tid):
        # check if we already have that topic
        for idx in range(MQTTSN_MAX_BROKER_TOPICS):
            if self.topics[idx].tid == tid:
                return self.topics[idx].name

        return b''

    def _get_instance(self, addr):
        for clnt in self.clients:
            if clnt.address == addr:
                return clnt

        return None

    def _handle_register(self, pkt, from_addr):
        clnt = self._get_instance(from_addr)
        if not clnt:
            return

        # unpack the msg
        msg = MQTTSNMessageRegister()
        if not msg.unpack(pkt) or msg.topic_id != 0x0000:
            return

        clnt.mark_time()

        # construct regack response
        reply = MQTTSNMessageRegack()
        reply.msg_id = msg.msg_id
        reply.return_code = MQTTSN_RC_ACCEPTED

        # get an ID and try to add the topic to the instance
        tid = self._get_topic_id(msg.topic_name)
        if not clnt.add_pub_topic(tid):
            reply.return_code = MQTTSN_RC_CONGESTION
        else:
            reply.topic_id = tid

        # now send our reply
        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

    def _handle_publish(self, pkt, from_addr):
        # check that we know this client
        if not self._get_instance(from_addr):
            return

        # now unpack the message, only QoS 0 for now
        msg = MQTTSNMessagePublish()
        if not msg.unpack(pkt) or msg.msg_id != 0x0000:
            return

        # get the topic name
        topic_name = self._get_topic_name(msg.topic_id)

        # if we're connected to the MQTT broker, just pass on the PUBLISH
        if self.mqttc and self.connected:
            self.mqttc.publish(topic_name, msg.data, msg.flags.qos, msg.flags.retain)
        else:
            # else we're on our own, add the msg to our queue
            # so we'll distribute it locally as broker
            self.pub_queue.append(pkt)

    def _handle_subscribe(self, pkt, from_addr):
        # get the right instance for this client
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
        if self.mqttc:
            self.mqttc.subscribe(msg.topic_id_name, msg.flags.qos)

    def _handle_unsubscribe(self, pkt, from_addr):
        clnt = self._get_instance(from_addr)
        if not clnt:
            return

        clnt.mark_time()

        # unpack the msg
        msg = MQTTSNMessageUnsubscribe()
        if not msg.unpack(pkt):
            return

        # construct unsuback response
        reply = MQTTSNMessageUnsuback()
        reply.msg_id = msg.msg_id

        # try to remove the topic from the instance
        if not clnt.delete_sub_topic(msg.topic_id_name):
            return

        # now send our reply
        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

        # here we issue our unsubscribe to MQTT clnt
        if self.mqttc:
            self.mqttc.unsubscribe(msg.topic_id_name)

    def _handle_pingreq(self, pkt, from_addr):
        clnt = self._get_instance(from_addr)
        if not clnt:
            return

        msg = MQTTSNMessagePingreq()
        if not msg.unpack(pkt):
            return

        clnt.mark_time()

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

        # now that we just reconnected to MQTT broker,
        # re-subscribe to all sub topics of all our MQTT-SN clients
        self.connected = True
        for clnt in self.clients:
            for topic in clnt.sub_topics:
                topic_name = self._get_topic_name(topic.tid)
                self.mqttc.subscribe(topic_name, topic.flags.qos)

    def _handle_mqtt_publish(self, topic: bytes, payload: bytes, flags: MQTTSNFlags):
        # adapt for qos 1 later with msg id
        
        # create a message and format it
        msg = MQTTSNMessagePublish()
        
        msg.data = payload
        topic_id = self._get_topic_id(topic)
        
        if topic_id == 0:
            return 
        
        msg.topic_id = topic_id
        msg.flags = flags
        
        # serialize and add to our pub queue
        pkt = msg.pack()
        self.pub_queue.append(pkt)
