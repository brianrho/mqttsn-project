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

        # store connect msg so we can use it when retrying
        self.connect_inflight = None
        self.connected = False

        self.state_handlers = []
        self.msg_handlers = []

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

        self.register_inflight = None
        self.topics = [None] * MQTTSN_MAX_NUM_TOPICS

    def add_gateway(self, gwid, gwaddr):
        self.gateway_list.append(MQTTSNGWInfo(gwid, gwaddr))

    def loop(self):
        # make sure to handle msgs first
        # so that the updated states get selected
        self._handle_messages()

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

    def _connect_in_progress_handler(self):
        # check if we've timed out after Nretry * Tretry secs
        if time.time() - self.unicast_timer >= MQTTSN_T_RETRY:
            self.unicast_timer = time.time()
            self.unicast_counter += 1
            if self.unicast_counter > MQTTSN_N_RETRY:
                self.connected = False
                self.state = MQTTSNState.LOST

                # Mark gateway as unavailable
                for i in range(len(self.gateway_list)):
                    if self.gateway_list[i].gwid == self.curr_gateway.gwid:
                        self.gateway_list[i].available = False
                        break

                return False

            # resend connect
            raw = self.connect_inflight.pack()
            self.transport.write_packet(raw, self.curr_gateway.gwaddr)
            return False

    def _handle_connack(self, pkt, from_addr):
        # make sure its from the right gateway
        # to be reviewed later, this cmp may not be worth it
        if not self.curr_gateway or from_addr.bytes != self.curr_gateway.gwaddr.bytes:
            return False

        msg = MQTTSNMessageConnack()
        if not msg.unpack(pkt):
            return False
        if msg.return_code != MQTTSN_RC_ACCEPTED:
            self.connect_inflight = None
            self.state = MQTTSNState.DISCONNECTED
            return False

        self.state = MQTTSNState.ACTIVE
        self.connected = True
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
        self.connect_inflight = MQTTSNMessageConnect()
        self.connect_inflight.flags = flags
        self.connect_inflight.client_id = self.client_id
        self.connect_inflight.duration = duration
        self.keep_alive_duration = duration

        # check if we have the target gw in our list
        raw = self.connect_inflight.pack()
        for info in self.gateway_list:
            if info.gwid == gwid:
                self.curr_gateway = info
                break
        else:
            self.connect_inflight = None
            return False

        # send the msg to the gw, start timers
        self.transport.write_packet(raw, self.curr_gateway.gwaddr)
        self.state = MQTTSNState.CONNECT_IN_PROGRESS
        self.unicast_timer = time.time()
        self.unicast_counter = 0
        return True

    def register_topic(self, topic):
        # if we're not connected or theres a pending register
        if not self.is_connected() or self.register_inflight:
            return False

        self.register_inflight = MQTTSNMessageRegister()
        self.register_inflight.topic_name = topic
        self.register_inflight.topic_id = 0
        # 0 is reserved for message IDs
        self.curr_msg_id = self.curr_msg_id + 1 if self.curr_msg_id == 0 else self.curr_msg_id
        self.register_inflight.msg_id = self.curr_msg_id

        raw = self.register_inflight.pack()
        self.transport.write_packet(raw, self.curr_gateway.gwaddr)
        self.unicast_timer = time.time()
        self.unicast_counter = 0

        # always a 16-bit value
        self.curr_msg_id = (self.curr_msg_id + 1) & 0xFFFF

    def _handle_regack(self, pkt, from_addr):
        # if this is to be used as proof of connectivity,
        # then we must verify that the gateway is the right one
        if not self.curr_gateway or from_addr.bytes != self.curr_gateway.gwaddr.bytes:
            return False

        msg = MQTTSNMessageRegack()
        if not msg.unpack(pkt):
            return False
        if msg.return_code != MQTTSN_RC_ACCEPTED:
            return False

        self.last_transaction = time.time()
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

    def ping(self):
        if not self.connected:
            return

        msg = MQTTSNMessagePingreq()
        raw = msg.pack()
        self.transport.write_packet(raw, self.curr_gateway.gwaddr)
        self.pingreq_timer = time.time()

    def is_connected(self):
        if self.connected:
            return True

        return self.loop()
