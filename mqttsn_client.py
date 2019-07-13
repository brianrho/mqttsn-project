from typing import List, Callable

from mqttsn_messages import *
from mqttsn_transport import MQTTSNTransport
from enum import IntEnum, unique
import time
import random
import logging


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


class MQTTSNPubTopic:
    def __init__(self, name, tid=0):
        self.name = name
        self.tid = tid


class MQTTSNSubTopic:
    def __init__(self, name, tid=0, flags=None):
        self.name = name
        self.tid = tid
        self.flags = flags if flags else MQTTSNFlags()


class MQTTSNClient:
    # list of topics
    pub_topics: List[MQTTSNPubTopic] = []
    sub_topics: List[MQTTSNSubTopic] = []

    # list of discovered gateways
    gateways: List[MQTTSNGWInfo] = []

    # publish callback
    publish_cb: Callable[[bytes, bytes, MQTTSNFlags], None] = None

    # handle incoming messages
    msg_handlers: List[Callable[[bytes, bytes], None]] = [None] * len(MQTTSN_MSG_TYPES)

    # handle client states
    state_handlers: List[Callable[[], None]] = [None] * len(MQTTSNState.__members__)

    def __init__(self, client_id, transport: MQTTSNTransport):
        self.transport = transport
        self.client_id = client_id[:MQTTSN_MAX_CLIENTID_LEN]
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
        self.msg_handlers[ADVERTISE] = self._handle_advertise
        self.msg_handlers[SEARCHGW] = self._handle_searchgw
        self.msg_handlers[GWINFO] = self._handle_gwinfo
        self.msg_handlers[CONNACK] = self._handle_connack
        self.msg_handlers[REGACK] = self._handle_regack
        self.msg_handlers[SUBACK] = self._handle_suback
        self.msg_handlers[UNSUBACK] = self._handle_unsuback
        self.msg_handlers[PUBLISH] = self._handle_publish
        self.msg_handlers[PINGRESP] = self._handle_pingresp

        self.state_handlers[MQTTSNState.ACTIVE] = self._active_handler
        self.state_handlers[MQTTSNState.SEARCHING] = self._searching_handler
        self.state_handlers[MQTTSNState.CONNECTING] = self._connecting_handler
        self.state_handlers[MQTTSNState.LOST] = self._lost_handler
        self.state_handlers[MQTTSNState.DISCONNECTED] = self._disconnected_handler

    def add_gateways(self, gateways: List[MQTTSNGWInfo]):
        self.gateways = gateways
        self.num_gateways = len(gateways)

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
            logging.debug('Retrying inflight msg => {}'.format(self.curr_gateway.gwid))

            if self.unicast_counter > MQTTSN_N_RETRY:
                self.connected = False
                self.msg_inflight = None
                self.state = MQTTSNState.LOST
                logging.debug('Gateway {} lost.'.format(self.curr_gateway.gwid))

                # Mark the gateway as unavailable
                self.curr_gateway.available = False

                return False

            # resend msg
            self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)
            return False

    def searchgw(self):
        # queue the search to begin after a random amount of time
        self.searchgw_started = time.time()
        self.searchgw_interval = random.uniform(0, MQTTSN_T_SEARCHGW)
        self.searchgw_pending = True
        self.state = MQTTSNState.SEARCHING
        logging.debug('Starting SEARCHGW delay for {} secs'.format(self.searchgw_interval))

    def connect(self, gwid=0, flags=None, duration=MQTTSN_DEFAULT_KEEPALIVE):
        if not self.gateways or self.msg_inflight:
            return False
        
        msg = MQTTSNMessageConnect()
        msg.flags = flags if flags else MQTTSNFlags()
        msg.client_id = self.client_id
        msg.duration = duration
        self.keep_alive_duration = duration
        
        # pack and store the msg for later retries
        self.msg_inflight = msg.pack()
        
        # check if a valid GWID was passed, 0 is reserved
        if gwid:
            # check if we have the desired gw in our list
            for info in self.gateways:
                if info.gwid == gwid:
                    self.curr_gateway = info
                    break
            else:
                self.msg_inflight = None
                return False
        else:
            # if no gateway was provided, select any available gateway
            logging.debug('Selecting gateway automatically.')
            for i in range(self.num_gateways):
                if self.gateways[i].gwid and self.gateways[i].available:
                    self.curr_gateway = self.gateways[i]
                    break
            else:
                # if they're all marked unavailable, lets try them all again
                for i in range(self.num_gateways):
                    self.gateways[i].available = True

                # try to search again for a valid gw
                for i in range(self.num_gateways):
                    if self.gateways[i].gwid:
                        self.curr_gateway = self.gateways[0]
                        break
                else:
                    self.msg_inflight = None
                    return False

        # send the msg to the gw, start timers
        self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)
        logging.debug('CONNECT => {}'.format(self.curr_gateway.gwid))

        self.connected = False
        self.state = MQTTSNState.CONNECTING

        # start unicast timer
        self.last_out = time.time()
        self.unicast_timer = time.time()
        self.unicast_counter = 0
        return True

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

        return True

    def _register(self, topic: MQTTSNPubTopic):
        msg = MQTTSNMessageRegister()
        msg.topic_name = topic.name
        msg.topic_id = 0
        # 0 is reserved for message IDs
        self.curr_msg_id = 1 if self.curr_msg_id == 0 else self.curr_msg_id
        msg.msg_id = self.curr_msg_id

        self.msg_inflight = msg.pack()
        self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)
        logging.debug('REGISTER {} => {}'.format(msg.topic_name, self.curr_gateway.gwid))

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

        flags = flags if flags else MQTTSNFlags()

        # msgid = 0 for qos 0
        if flags.qos in (1, 2):
            # 0 is reserved
            self.curr_msg_id = 1 if self.curr_msg_id == 0 else self.curr_msg_id
            msg.msg_id = self.curr_msg_id

            # increment, always a 16-bit value
            self.curr_msg_id = (self.curr_msg_id + 1) & 0xFFFF

        msg.flags = flags
        msg.data = data
        raw = msg.pack()
        self.transport.write_packet(raw, self.curr_gateway.gwaddr)
        logging.debug('PUBLISH {} to topic {} => {}'.format(msg.data, topic, self.curr_gateway.gwid))

        # need to note last_out for QoS 1 publish

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
        logging.debug('SUBSCRIBE to topic {} => {}'.format(topic, self.curr_gateway.gwid))

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
        logging.debug('UNSUBSCRIBE to topic {} => {}'.format(topic, self.curr_gateway.gwid))

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
        logging.debug('PINGREQ => {}'.format(self.curr_gateway.gwid))

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
        logging.debug('DISCONNECT => {}'.format(self.curr_gateway.gwid))

        self.connected = False
        self.state = MQTTSNState.DISCONNECTED

    def on_message(self, callback):
        self.publish_cb = callback

    def _handle_advertise(self, pkt, from_addr):
        msg = MQTTSNMessageAdvertise()
        if not msg.unpack(pkt):
            return

        logging.debug('ADVERTISE by ID {}, ADDR {}'.format(msg.gwid, from_addr))

        for i in range(self.num_gateways):
            if self.gateways[i].gwid == 0:
                self.gateways[i].gwid = msg.gwid
                self.gateways[i].gwaddr = from_addr
                break

        return

    def _handle_searchgw(self, pkt, from_addr):
        msg = MQTTSNMessageSearchGW()
        if not msg.unpack(pkt):
            return

        logging.debug('SEARCHGW from {}'.format(from_addr))

        # in state handler, we fire the actual message
        # and check timers to know if we should resend

        # Here, we just cancel our pending request and reset timer
        # if some other client already sent it
        if self.searchgw_pending:
            logging.debug('Cancelling SEARCHGW')
            self.searchgw_pending = False
            self.searchgw_started = time.time()

        # TODO: Send GWINFO from clients

        return

    def _handle_gwinfo(self, pkt, from_addr):
        msg = MQTTSNMessageGWInfo()
        if not msg.unpack(pkt):
            return

        logging.debug('GWINFO <= {}'.format(from_addr))

        # check if its in our gateway list, add it if its not
        for info in self.gateways:
            if info.gwid == msg.gwid:
                break
        else:
            for i in range(self.num_gateways):
                if self.gateways[i].gwid == 0:
                    self.gateways[i].gwid = msg.gwid

                    # check if a gw or client sent the GWINFO
                    self.gateways[i].gwaddr = msg.gwadd if msg.gwadd else from_addr
                    break

        # we've gotten a GWINFO but we'll remain in this state
        # till app tries to connect
        self.searchgw_pending = False
        # self.state = MQTTSNState.DISCONNECTED

    def _handle_connack(self, pkt, from_addr):
        # make sure its from the solicited gateway
        if not self.curr_gateway or from_addr != self.curr_gateway.gwaddr:
            return

        if self.msg_inflight is None:
            return

        # unpack the original connect
        header = MQTTSNHeader()
        hlen = header.unpack(self.msg_inflight)
        if not hlen or header.msg_type != CONNECT:
            return

        sent = MQTTSNMessageConnect()
        sent.unpack(self.msg_inflight[hlen:])

        # now unpack the connack
        msg = MQTTSNMessageConnack()
        if not msg.unpack(pkt):
            return
        if msg.return_code != MQTTSN_RC_ACCEPTED:
            self.msg_inflight = None
            self.state = MQTTSNState.DISCONNECTED
            return

        logging.debug('CONNACK <= {}'.format(from_addr))

        # we are now active
        self.state = MQTTSNState.ACTIVE
        self.connected = True
        self.msg_inflight = None
        self.last_in = time.time()

        # re-register and re-sub topics
        for topic in self.sub_topics:
            topic.tid = 0
        for topic in self.pub_topics:
            topic.tid = 0

    def _handle_regack(self, pkt, from_addr):
        # if this is to be used as proof of connectivity,
        # then we must verify that the gateway is the right one
        if not self.curr_gateway or from_addr != self.curr_gateway.gwaddr:
            return

        if self.msg_inflight is None:
            return

        # unpack the original message
        header = MQTTSNHeader()
        hlen = header.unpack(self.msg_inflight)
        if header.msg_type != REGISTER:
            return

        sent = MQTTSNMessageRegister()
        sent.unpack(self.msg_inflight[hlen:])

        # now unpack the response
        msg = MQTTSNMessageRegack()
        if not msg.unpack(pkt):
            return
        if msg.msg_id != sent.msg_id or msg.return_code != MQTTSN_RC_ACCEPTED:
            return

        logging.debug('REGACK for topic {} ID {} <= {}'.format(sent.topic_name, msg.topic_id, from_addr))

        for i in range(self.pub_topics_cnt):
            if self.pub_topics[i].name == sent.topic_name:
                self.pub_topics[i].tid = msg.topic_id
                break
        else:
            # topic not found
            return

        self.msg_inflight = None
        self.last_in = time.time()

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

        logging.debug('PUBLISH for topic {} ID {} <= {}'.format(topic, msg.topic_id, from_addr))

        # call user handler
        self.publish_cb(topic, msg.data, msg.flags)

    # TODO: Consider removing suback and unsuback, no gain in parsing them
    def _handle_suback(self, pkt, from_addr):
        # if this is to be used as proof of connectivity,
        # then we must verify that the gateway is the right one
        if not self.curr_gateway or from_addr != self.curr_gateway.gwaddr:
            return False

        if self.msg_inflight is None:
            return False

        # unpack the original message
        header = MQTTSNHeader()
        hlen = header.unpack(self.msg_inflight)
        if header.msg_type != SUBSCRIBE:
            return

        sent = MQTTSNMessageSubscribe()
        sent.unpack(self.msg_inflight[hlen:])

        # now unpack the response
        msg = MQTTSNMessageSuback()
        if not msg.unpack(pkt):
            return
        if msg.msg_id != sent.msg_id or msg.return_code != MQTTSN_RC_ACCEPTED:
            return

        logging.debug('SUBACK for topic {} ID {} <= {}'.format(sent.topic_id_name, msg.topic_id, from_addr))

        # update the topic id
        for i in range(self.sub_topics_cnt):
            if self.sub_topics[i].name == sent.topic_id_name:
                self.sub_topics[i].tid = msg.topic_id
                break
        else:
            # topic not found
            return

        self.msg_inflight = None
        self.last_in = time.time()

    def _handle_unsuback(self, pkt, from_addr):
        # if this is to be used as proof of connectivity,
        # then we must verify that the gateway is the right one
        if not self.curr_gateway or from_addr != self.curr_gateway.gwaddr:
            return

        if self.msg_inflight is None:
            return

        # unpack the original message
        header = MQTTSNHeader()
        hlen = header.unpack(self.msg_inflight)
        if header.msg_type != UNSUBSCRIBE:
            return

        sent = MQTTSNMessageUnsubscribe()
        sent.unpack(self.msg_inflight[hlen:])

        # now unpack the response
        msg = MQTTSNMessageUnsuback()
        if not msg.unpack(pkt):
            return
        if msg.msg_id != sent.msg_id:
            return

        logging.debug('UNSUBACK for topic {} <= {}'.format(sent.topic_id_name, from_addr))

        # remove from list
        for i in range(self.sub_topics_cnt):
            if self.sub_topics[i].name == sent.topic_id_name:
                self.sub_topics[i].name = ''
                self.sub_topics[i].tid = 0
                break
        else:
            return

        self.msg_inflight = None
        self.last_in = time.time()

    def _handle_pingresp(self, pkt, from_addr):
        # if this is to be used as proof of connectivity,
        # then we must verify that the gateway is the right one
        if not self.curr_gateway or from_addr != self.curr_gateway.gwaddr:
            return

        msg = MQTTSNMessagePingresp()
        if not msg.unpack(pkt):
            return

        logging.debug('PINGRESP <= {}'.format(from_addr))

        self.last_in = time.time()
        self.pingresp_pending = False
        return

    def _searching_handler(self):
        # if there's a pending search and the wait interval is over
        if self.searchgw_pending and time.time() > self.searchgw_started + self.searchgw_interval:
            # broadcast it and start waiting again
            msg = MQTTSNMessageSearchGW().pack()
            self.transport.broadcast(msg)
            self.searchgw_started = time.time()
            self.searchgw_interval = random.uniform(0, MQTTSN_T_SEARCHGW)
            logging.debug('SEARCHGW broadcast. Next up in {} secs'.format(self.searchgw_interval))

    def _connecting_handler(self):
        if self.connected:
            self.state = MQTTSNState.ACTIVE
            return

    def _lost_handler(self):
        if self.connected:
            self.state = MQTTSNState.ACTIVE
            return

        # try to re-connect to the gateway
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
                    # we are now lost and the gateway is unavailable
                    self.state = MQTTSNState.LOST
                    self.curr_gateway.available = False
                    self.connected = False
                    self.pingresp_pending = False
                    logging.debug('Gateway {} lost.'.format(self.curr_gateway.gwid))
                else:
                    # if we still have time, keep pinging
                    logging.debug('Retrying PING.')
                    self.ping()
                    self.pingreq_timer = time.time()
                    self.last_out = time.time()
