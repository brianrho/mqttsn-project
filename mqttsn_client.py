from structures import *
from mqttsn_transport import MQTTSNTransport, MQTTSNAddress
from enum import Enum
import time
import random


class MQTTSNState(Enum):
    ACTIVE = 0
    LOST = 1
    ASLEEP = 2
    AWAKE = 3
    DISCONNECTED = 4

    CONNECT_IN_PROGRESS = 5
    SEARCHING = 6


class MQTTSNGWInfo:
    def __init__(self, gwid, gwaddr: MQTTSNAddress):
        self.gwid = gwid
        self.gwaddr = gwaddr
        self.available = True


class MQTTSNClient:
    def __init__(self, client_id, transport: MQTTSNTransport):
        self.transport = transport
        self.client_id = client_id
        self.state = MQTTSNState.LOST

        # store list of gateways and the current gw
        self.gateway_list = []
        self.curr_gateway = None

        self.connected = False

        self.state_handlers = []
        self.msg_handlers = []

        # store unicast msg so we can use it when retrying
        self.msg_inflight = None
        self.unicast_timer = time.time()
        self.unicast_counter = 0

        self.keep_alive_duration = MQTTSN_DEFAULT_KEEPALIVE
        self.last_transaction = 0

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

        self.topics = [None] * MQTTSN_MAX_NUM_TOPICS
        self.num_topics = 0

    def add_gateway(self, gwid, gwaddr):
        self.gateway_list.append(MQTTSNGWInfo(gwid, gwaddr))

    def loop(self):
        # make sure to handle msgs first
        # so that the updated states get selected
        self._handle_messages()

        # check up on any messages awaiting a reply
        self._inflight_handler()

        # run state handler
        state_val = self.state.value
        self.state_handlers[state_val]()

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

                # Mark gateway as unavailable
                for i in range(len(self.gateway_list)):
                    if self.gateway_list[i].gwid == self.curr_gateway.gwid:
                        self.gateway_list[i].available = False
                        break

                return False

            # resend msg
            self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)
            return False

    def _handle_connack(self, pkt, from_addr):
        if not self.curr_gateway or from_addr.bytes != self.curr_gateway.gwaddr.bytes:
            return False

        if self.msg_inflight is None:
            return False

        # unpack the original connect
        header = MQTTSNHeader()
        hlen = header.unpack(self.msg_inflight)
        if header.msg_type != MQTTSNMessageConnect:
            return False

        sent = MQTTSNMessageConnect()
        sent.unpack(self.msg_inflight[hlen:])

        # now unpack the connack
        msg = MQTTSNMessageConnack()
        if not msg.unpack(pkt):
            return False
        if msg.return_code != MQTTSN_RC_ACCEPTED:
            self.msg_inflight = None
            self.state = MQTTSNState.DISCONNECTED
            return False

        self.state = MQTTSNState.ACTIVE
        self.connected = True
        self.msg_inflight = None
        self.last_transaction = time.time()
        return True

    def _handle_advertise(self, pkt, from_addr):
        msg = MQTTSNMessageAdvertise()
        if not msg.unpack(pkt):
            return False

        # check if its in our gateway list, add it if its not
        for info in self.gateway_list:
            if info.gwid == msg.gwid:
                break
        else:
            self.gateway_list.append(MQTTSNGWInfo(msg.gwid, from_addr))
        return True

    def searchgw(self):
        self.searchgw_started = time.time()
        self.searchgw_interval = random.uniform(0, MQTTSN_T_SEARCHGW)
        self.searchgw_pending = True
        self.state = MQTTSNState.SEARCHING

    def _handle_searchgw(self, pkt, from_addr):
        msg = MQTTSNMessageSearchGW()
        if not msg.unpack(pkt):
            return False

        # in state handler, we fire the actual message
        # and check timers to know if we should resend

        # Here, we just cancel our pending request and reset timer
        # if some other client already sent it
        if self.searchgw_pending:
            self.searchgw_pending = False
            self.searchgw_started = time.time()

        # TODO: Send GWINFO from clients

        return True

    def _handle_gwinfo(self, pkt, from_addr):
        msg = MQTTSNMessageGWInfo()
        if not msg.unpack(pkt):
            return False

        # check if its in our gateway list, add it if its not
        for info in self.gateway_list:
            if info.gwid == msg.gwid:
                break
        else:
            # check if a gw or client sent the GWINFO
            if msg.gwadd:
                self.gateway_list.append(MQTTSNGWInfo(msg.gwid, MQTTSNAddress(msg.gwadd)))
            else:
                self.gateway_list.append(MQTTSNGWInfo(msg.gwid, from_addr))

        return True

    def connect(self, gwid=0, flags=0, duration=MQTTSN_DEFAULT_KEEPALIVE):
        if self.msg_inflight:
            return False
        
        msg = MQTTSNMessageConnect()
        msg.flags = flags
        msg.client_id = self.client_id
        msg.duration = duration
        self.keep_alive_duration = duration

        # check if we have the target gw in our list
        self.msg_inflight = msg.pack()
        for info in self.gateway_list:
            if info.gwid == gwid:
                self.curr_gateway = info
                break
        else:
            self.msg_inflight = None
            return False

        # send the msg to the gw, start timers
        self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)
        self.state = MQTTSNState.CONNECT_IN_PROGRESS
        self.unicast_timer = time.time()
        self.unicast_counter = 0
        return True

    def register_topic(self, topic):
        # if we're not connected or theres a pending register
        if not self.is_connected() or self.msg_inflight:
            return False

        msg = MQTTSNMessageRegister()
        msg.topic_name = topic
        msg.topic_id = 0
        # 0 is reserved for message IDs
        self.curr_msg_id = self.curr_msg_id + 1 if self.curr_msg_id == 0 else self.curr_msg_id
        msg.msg_id = self.curr_msg_id

        self.msg_inflight = msg.pack()
        self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)
        self.unicast_timer = time.time()
        self.unicast_counter = 0

        # always a 16-bit value
        self.curr_msg_id = (self.curr_msg_id + 1) & 0xFFFF

    def _handle_regack(self, pkt, from_addr):
        # if this is to be used as proof of connectivity,
        # then we must verify that the gateway is the right one
        if not self.curr_gateway or from_addr.bytes != self.curr_gateway.gwaddr.bytes:
            return False

        if self.msg_inflight is None:
            return False

        # unpack the original message
        header = MQTTSNHeader()
        hlen = header.unpack(self.msg_inflight)
        if header.msg_type != MQTTSNMessageRegister:
            return False

        sent = MQTTSNMessageRegister()
        sent.unpack(self.msg_inflight[hlen:])

        # now unpack the response
        msg = MQTTSNMessageRegack()
        if not msg.unpack(pkt):
            return False
        if msg.msg_id != sent.msg_id or msg.return_code != MQTTSN_RC_ACCEPTED:
            return False

        self.last_transaction = time.time()
        self.topics[self.num_topics].name = sent.topic_name
        self.topics[self.num_topics].name = msg.topic_id
        self.msg_inflight = None
        self.num_topics += 1
        return True

    def _handle_pingresp(self, pkt, from_addr):
        msg = MQTTSNMessagePingresp()
        if not msg.unpack(pkt):
            return False

        self.last_transaction = time.time()
        return True

    def _active_handler(self):
        # use this for now, so we can change fraction later
        duration = self.keep_alive_duration / 2

        if time.time() >= self.last_transaction + duration:
            if not self.pingresp_pending:
                self.ping()
                self.pingresp_pending = True
            elif time.time() - self.pingreq_timer >= MQTTSN_T_RETRY:
                # just keep trying till we hit the keepalive limit
                if time.time() >= self.last_transaction + self.keep_alive_duration:
                    self.state = MQTTSNState.LOST
                    self.connected = False
                    self.pingresp_pending = False
                else:
                    self.ping()
                    self.pingreq_timer = time.time()

    def publish(self, topic, data, flags=None):
        # if we're not connected
        if not self.is_connected():
            return False

        msg = MQTTSNMessagePublish()
        for t in self.topics:
            if t.name == topic:
                msg.topic_id = t.tid
                break
        else:
            return False

        # msgid = 0 for qos 0
        if flags.qos in (1, 2):
            # 0 is reserved for message IDs
            self.curr_msg_id = self.curr_msg_id + 1 if self.curr_msg_id == 0 else self.curr_msg_id
            msg.msg_id = self.curr_msg_id

        msg.flags = flags
        msg.data = data
        raw = msg.pack()
        self.transport.write_packet(raw, self.curr_gateway.gwaddr)

        # always a 16-bit value
        self.curr_msg_id = (self.curr_msg_id + 1) & 0xFFFF

    def subscribe(self, topic, flags=None):
        # if we're not connected or there's a pending transaction
        if not self.is_connected() or self.msg_inflight:
            return False

        msg = MQTTSNMessageSubscribe()
        msg.topic_id_name = topic

        # 0 is reserved for message IDs
        self.curr_msg_id = self.curr_msg_id + 1 if self.curr_msg_id == 0 else self.curr_msg_id
        msg.msg_id = self.curr_msg_id
        msg.flags = flags

        self.msg_inflight = msg.pack()
        self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)
        self.unicast_timer = time.time()
        self.unicast_counter = 0

        # always a 16-bit value
        self.curr_msg_id = (self.curr_msg_id + 1) & 0xFFFF

    def _handle_suback(self, pkt, from_addr):
        # if this is to be used as proof of connectivity,
        # then we must verify that the gateway is the right one
        if not self.curr_gateway or from_addr.bytes != self.curr_gateway.gwaddr.bytes:
            return False

        if self.msg_inflight is None:
            return False

        # unpack the original message
        header = MQTTSNHeader()
        hlen = header.unpack(self.msg_inflight)
        if header.msg_type != MQTTSNMessageSubscribe:
            return False

        sent = MQTTSNMessageSubscribe()
        sent.unpack(self.msg_inflight[hlen:])

        # now unpack the response
        msg = MQTTSNMessageSuback()
        if not msg.unpack(pkt):
            return False
        if msg.msg_id != sent.msg_id or msg.return_code != MQTTSN_RC_ACCEPTED:
            return False

        self.last_transaction = time.time()
        self.topics[self.num_topics].name = sent.topic_id_name
        self.topics[self.num_topics].name = msg.topic_id
        self.msg_inflight = None
        self.num_topics += 1
        return True

    def unsubscribe(self, topic, flags=None):
        # if we're not connected or there's a pending transaction
        if not self.is_connected() or self.msg_inflight:
            return False

        msg = MQTTSNMessageUnsubscribe()

        for t in self.topics:
            if t.name == topic:
                msg.topic_id_name = topic
                break
        else:
            return False

        # 0 is reserved
        self.curr_msg_id = self.curr_msg_id + 1 if self.curr_msg_id == 0 else self.curr_msg_id
        msg.msg_id = self.curr_msg_id
        msg.flags = flags

        self.msg_inflight = msg.pack()
        self.transport.write_packet(self.msg_inflight, self.curr_gateway.gwaddr)
        self.unicast_timer = time.time()
        self.unicast_counter = 0

        # always a 16-bit value
        self.curr_msg_id = (self.curr_msg_id + 1) & 0xFFFF

    def _handle_unsuback(self, pkt, from_addr):
        # if this is to be used as proof of connectivity,
        # then we must verify that the gateway is the right one
        if not self.curr_gateway or from_addr.bytes != self.curr_gateway.gwaddr.bytes:
            return False

        if self.msg_inflight is None:
            return False

        # unpack the original message
        header = MQTTSNHeader()
        hlen = header.unpack(self.msg_inflight)
        if header.msg_type != MQTTSNMessageUnsubscribe:
            return False

        sent = MQTTSNMessageUnsubscribe()
        sent.unpack(self.msg_inflight[hlen:])

        # now unpack the response
        msg = MQTTSNMessageUnsuback()
        if not msg.unpack(pkt):
            return False
        if msg.msg_id != sent.msg_id:
            return False

        self.last_transaction = time.time()

        # ##revise add and remove later
        for topic in self.topics:
            if topic.name == sent.topic_id_name:
                self.topics.remove(topic)
                break
        else:
            return False

        self.msg_inflight = None
        self.num_topics -= 1
        return True

    def ping(self):
        if not self.connected:
            return

        msg = MQTTSNMessagePingreq()
        raw = msg.pack()
        self.transport.write_packet(raw, self.curr_gateway.gwaddr)
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
