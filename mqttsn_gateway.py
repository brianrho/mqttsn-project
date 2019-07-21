from typing import List, Callable

from mqttsn_messages import *
from mqttsn_transport import MQTTSNTransport
from mqtt_client import MQTTClient
from enum import IntEnum, unique
import time
import collections
import logging


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
    DISCONNECTED = 2
    LOST = 3


class MQTTSNInstance:
    transport: MQTTSNTransport

    def __init__(self):
        self.sub_topics: List[MQTTSNInstanceSubTopic] = \
            [MQTTSNInstanceSubTopic() for _ in range(MQTTSN_MAX_INSTANCE_TOPICS)]
        self.pub_topics: List[MQTTSNInstancePubTopic] = \
            [MQTTSNInstancePubTopic() for _ in range(MQTTSN_MAX_INSTANCE_TOPICS)]

        self.cid: bytes = b''
        self.flags: MQTTSNFlags = MQTTSNFlags()

        self.address: bytes = b''
        self.msg_inflight: bytes = b''
        self.unicast_timer: float = 0
        self.unicast_counter: float = 0

        self.keepalive_duration: int = MQTTSN_DEFAULT_KEEPALIVE
        self.last_in: float = 0
        self.status: MQTTSNInstanceStatus = MQTTSNInstanceStatus.DISCONNECTED

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

    def add_sub_topic(self, tid: int, flags: MQTTSNFlags):
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

    def add_pub_topic(self, tid: int):
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
                return

    # no real use for this yet
    def delete_pub_topic(self, tid):
        for i in range(MQTTSN_MAX_INSTANCE_TOPICS):
            if self.pub_topics[i].tid == tid:
                self.pub_topics[i].tid = 0
                return

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


# a mapping of topic name to topic ID and type
class MQTTSNTopicMapping:
    def __init__(self, name=b'', tid=0, ttype=0):
        self.name = name
        self.tid = tid
        self.type = ttype
        self.subbed = False
        self.sub_qos = 0


class MQTTSNGateway:
    # for holding the broker's list of topic ID mappings
    mappings: List[MQTTSNTopicMapping] = [MQTTSNTopicMapping() for _ in range(MQTTSN_MAX_GATEWAY_TOPICS)]

    # list of clients
    clients: List[MQTTSNInstance] = [MQTTSNInstance() for _ in range(MQTTSN_MAX_NUM_CLIENTS)]

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
                logging.debug('Client {} lost'.format(clnt.address))
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
            if not msg.unpack(pkt[rlen:]) or msg.msg_id != 0x0000:
                continue

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

            # call the msg handler
            self.msg_handlers[idx](pkt[rlen:], from_addr)

    def _handle_searchgw(self, pkt, from_addr):
        msg = MQTTSNMessageSearchGW()
        if not msg.unpack(pkt):
            return

        logging.debug('SEARCHGW from {}'.format(from_addr))

        reply = MQTTSNMessageGWInfo()
        reply.id = self.gw_id
        raw = reply.pack()
        self.transport.broadcast(raw)

        logging.debug('GWINFO broadcast.')

    def _handle_connect(self, pkt, from_addr):
        msg = MQTTSNMessageConnect()
        if not msg.unpack(pkt) or not msg.client_id:
            return

        logging.info('CONNECT from {}'.format(from_addr))

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
        for idx in range(MQTTSN_MAX_GATEWAY_TOPICS):
            if self.mappings[idx].name == name:
                return self.mappings[idx].tid
                
        if len(name) > MQTTSN_MAX_TOPICNAME_LEN:
            return 0;
            
        # else, add it
        for idx in range(MQTTSN_MAX_GATEWAY_TOPICS):
            if not self.mappings[idx].name:
                self.mappings[idx].name = name
                self.mappings[idx].tid = idx + 1
                while self.mappings[idx].tid in (MQTTSN_TOPIC_UNSUBSCRIBED, MQTTSN_TOPIC_NOTASSIGNED):
                    self.mappings[idx].tid += 1
                return self.mappings[idx].tid

        return 0

    def get_topic_mapping(self, tid):
        # check if we already have that topic
        for idx in range(MQTTSN_MAX_GATEWAY_TOPICS):
            if self.mappings[idx].tid == tid:
                return self.mappings[idx]

        return None

    def _get_instance(self, addr):
        for clnt in self.clients:
            if clnt and clnt.address == addr:
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

        logging.debug('REGISTER {} from {}.'.format(msg.topic_name, from_addr))
        clnt.mark_time()

        # construct regack response
        reply = MQTTSNMessageRegack()
        reply.msg_id = msg.msg_id
        reply.return_code = MQTTSN_RC_ACCEPTED

        # get an ID and try to add the topic to the instance
        tid = self._get_topic_id(msg.topic_name)
        if not tid:
            return

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
        mapping = self.get_topic_mapping(msg.topic_id)
        if not mapping:
            return

        logging.debug('PUBLISH {} to topic {} from {}.'.format(msg.data, mapping.name, from_addr))

        # if we're connected to the MQTT broker, just pass on the PUBLISH
        if self.mqttc and self.connected:
            self.mqttc.publish(mapping.name, msg.data, msg.flags.qos, msg.flags.retain)
        else:
            # else we're on our own, add the msg to our queue
            # so we'll distribute it locally as broker
            self.pub_queue.append(msg.pack())

    def _handle_subscribe(self, pkt, from_addr):
        # get the right instance for this client
        clnt = self._get_instance(from_addr)
        if not clnt:
            return

        # unpack the msg
        msg = MQTTSNMessageSubscribe()
        if not msg.unpack(pkt):
            return

        clnt.mark_time()
        logging.debug('SUBSCRIBE to topic {} from {}.'.format(msg.topic_id_name, from_addr))
        
        # construct suback response
        reply = MQTTSNMessageSuback()
        reply.msg_id = msg.msg_id
        reply.return_code = MQTTSN_RC_ACCEPTED

        # get an ID
        tid = self._get_topic_id(msg.topic_id_name)
        if not tid:
            return

        reply.return_code = MQTTSN_RC_ACCEPTED
        # add the topic to the instance
        if not clnt.add_sub_topic(tid, msg.flags):
            reply.return_code = MQTTSN_RC_CONGESTION
        else:
            reply.topic_id = tid

        # now send our reply
        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

        # send the new sub to MQTT broker
        if reply.return_code == MQTTSN_RC_ACCEPTED:
            self.add_subscription(tid, msg.flags.qos)

    def add_subscription(self, tid, qos):
        mapping = self.get_topic_mapping(tid)

        # if there's no MQTT sub yet
        if not mapping.subbed:
            mapping.subbed = True
            mapping.sub_qos = qos
            if self.mqttc and self.connected:
                self.mqttc.subscribe(mapping.name, qos)
        # or if the new sub has a higher qos
        elif mapping.sub_qos < qos:
            mapping.sub_qos = qos
            if self.mqttc and self.connected:
                self.mqttc.subscribe(mapping.name, qos)

    def delete_subscription(self, tid):
        mapping = self.get_topic_mapping(tid)
        mapping.subbed = False
        mapping.sub_qos = 0
        if self.mqttc and self.connected:
            self.mqttc.unsubscribe(mapping.name)

    def _handle_unsubscribe(self, pkt, from_addr):
        clnt = self._get_instance(from_addr)
        if not clnt:
            return

        # unpack the msg
        msg = MQTTSNMessageUnsubscribe()
        if not msg.unpack(pkt):
            return

        clnt.mark_time()
        logging.debug('UNSUBSCRIBE to topic {} from {}.'.format(msg.topic_id_name, from_addr))

        # construct unsuback response
        reply = MQTTSNMessageUnsuback()
        reply.msg_id = msg.msg_id

        # get the topic ID first
        tid = self._get_topic_id(msg.topic_id_name)
        if not tid:
            return

        # delete the topic from the instance
        clnt.delete_sub_topic(tid)

        # now send our reply
        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

        # check if anybody's still subscribed
        for clnt in self.clients:
            if clnt.is_subbed(tid):
                return

        # if not, delete the sub from MQTT broker
        self.delete_subscription(tid)

    def _handle_pingreq(self, pkt, from_addr):
        clnt = self._get_instance(from_addr)
        if not clnt:
            return

        msg = MQTTSNMessagePingreq()
        if not msg.unpack(pkt):
            return

        clnt.mark_time()
        logging.debug('PINGREQ from {}'.format(from_addr))

        # now send our reply
        reply = MQTTSNMessagePingresp()
        raw = reply.pack()
        self.transport.write_packet(raw, from_addr)

    def _handle_mqtt_conn(self, conn_state: bool):
        # now we know we're no longer connected to MQTT broker
        if not conn_state:
            logging.debug('MQTT disconnected.')
            self.connected = False
            return

        if self.connected:
            return

        logging.debug('MQTT connected.')
        # now that we just reconnected to MQTT broker,
        # re-subscribe to all sub topics of all our MQTT-SN clients
        self.connected = True
        for mapping in self.mappings:
            if mapping.subbed:
                self.mqttc.subscribe(mapping.name, mapping.sub_qos)

    def _handle_mqtt_publish(self, topic: bytes, payload: bytes, flags: MQTTSNFlags):
        # adapt for qos 1 later with msg id

        logging.debug('MQTT-PUBLISH {} to {}'.format(payload, topic))

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
