import struct
from mqttsn_defines import *

# Message types
MQTTSN_MSG_TYPES = range(30)

ADVERTISE, SEARCHGW, GWINFO, reserved, CONNECT, CONNACK, \
    WILLTOPICREQ, WILLTOPIC, WILLMSGREQ, WILLMSG, \
    REGISTER, REGACK, PUBLISH, PUBACK, PUBCOMP, PUBREC, \
    PUBREL, reserved1, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, \
    PINGREQ, PINGRESP, DISCONNECT, reserved2, \
    WILLTOPICUPD, WILLTOPICRESP, WILLMSGUPD, WILLMSGRESP = MQTTSN_MSG_TYPES

MSG_TYPE_NAMES = ["ADVERTISE", "SEARCHGW", "GWINFO", "reserved",
                  "CONNECT", "CONNACK", "WILLTOPICREQ", "WILLTOPIC",
                  "WILLMSGREQ", "WILLMSG", "REGISTER", "REGACK",
                  "PUBLISH", "PUBACK", "PUBCOMP", "PUBREC", "PUBREL",
                  "reserved", "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE",
                  "UNSUBACK", "PINGREQ", "PINGRESP", "DISCONNECT",
                  "reserved", "WILLTOPICUPD", "WILLTOPICRESP",
                  "WILLMSGUPD", "WILLMSGRESP"]

TOPIC_TYPE_NAMES = ["NORMAL", "PREDEFINED", "SHORT_NAME"]
MQTTSN_TOPIC_NORMAL, MQTTSN_TOPIC_PREDEFINED, MQTTSN_TOPIC_SHORTNAME = range(3)

# return codes
MQTTSN_RC_ACCEPTED = 0x00
MQTTSN_RC_CONGESTION = 0x01
MQTTSN_RC_INVALIDTID = 0x02
MQTTSN_RC_NOTSUPPORTED = 0x03


class MQTTSNHeader:
    def __init__(self, msg_type=None):
        self.length = 0
        self.msg_type = msg_type

    # return a buffer containing the header i.e. length + msg type
    def pack(self, length=0):
        self.length = length + MQTTSN_HEADER_LEN

        # encode header with single-byte length field
        if self.length < 256:
            return struct.pack(">BB", self.length, self.msg_type)
        else:
            return b''

    # parse buffer contents and return the length consumed
    def unpack(self, buffer):
        # check buffer length and LENGTH field is non-zero
        if len(buffer) < MQTTSN_HEADER_LEN or buffer[0] == 0:
            return 0

        # multi-byte length (> 255) not supported
        if buffer[0] == 0x01:
            return 0

        # subtract 2 from total length to get length of variable field
        self.length = buffer[0] - MQTTSN_HEADER_LEN
        self.msg_type = buffer[1]
        return MQTTSN_HEADER_LEN


class MQTTSNFlags:
    def __init__(self):
        self.dup = 0
        self.qos = 0
        self.retain = 0
        self.will = 0
        self.clean_session = 0
        self.topicid_type = 0
        self.union = 0

    # return a byte containing the flags
    def pack(self):
        value = (self.dup << 7) | (self.qos << 5) | (self.retain << 4) | \
                (self.will << 3) | (self.clean_session << 2) | self.topicid_type

        self.union = value
        return bytes([value])

    # parse a byte from the buffer and return the length consumed
    def unpack(self, buffer):
        self.union = buffer[0]
        self.dup = self.union >> 7
        self.qos = (self.union >> 5) & 0x3
        self.retain = (self.union >> 4) & 0x1
        self.will = (self.union >> 3) & 0x1
        self.clean_session = (self.union >> 2) & 0x1
        self.topicid_type = self.union & 0x3
        return 1

    def __str__(self):
        return str(self.union)


# each child must implement a pack() that returns a filled buffer
# and an unpack() that fills the instance attributes
# Where convenient, init() takes arguments for attribute initialization
class MQTTSNMessage:
    def __init__(self):
        self.header = None

    # @abc.abstractmethod
    def pack(self):
        return b''

    # @abc.abstractmethod
    def unpack(self, buffer):
        return False

    def __eq__(self, other):
        for attr in self.__dict__:
            s1 = str(getattr(self, attr))
            s2 = str(getattr(other, attr))
            if s1 != s2:
                print("Unequal: ", s1, s2)
                return False
        return True
        # return self.__dict__ == other.__dict__

    def __str__(self):
        return str(self.__dict__)


class MQTTSNMessageAdvertise(MQTTSNMessage):
    def __init__(self, gwid=0):
        super().__init__()
        self.gwid = gwid
        self.duration = 0

    def pack(self):
        header = MQTTSNHeader(ADVERTISE)
        msg = header.pack(3)
        msg += struct.pack(">BH", self.gwid, self.duration)
        return msg

    def unpack(self, buffer):
        try:
            self.gwid, self.duration = struct.unpack(">BH", buffer)
            return True
        except struct.error:
            return False


class MQTTSNMessageSearchGW(MQTTSNMessage):
    def __init__(self, radius=0):
        super().__init__()
        self.radius = radius

    def pack(self):
        header = MQTTSNHeader(SEARCHGW)
        msg = header.pack(1)
        msg += struct.pack("B", self.radius)
        return msg

    def unpack(self, buffer):
        try:
            self.radius = struct.unpack("B", buffer)[0]
            return True
        except struct.error:
            return False


class MQTTSNMessageGWInfo(MQTTSNMessage):
    def __init__(self, gwid=0):
        super().__init__()
        self.gwid = gwid
        self.gwadd = b''

    def pack(self):
        header = MQTTSNHeader(GWINFO)
        self.gwadd = self.gwadd[:GW_ADDR_LENGTH]

        msg = header.pack(1 + len(self.gwadd))
        msg += struct.pack(">B{}s".format(len(self.gwadd)), self.gwid, self.gwadd)
        return msg

    def unpack(self, buffer):
        fmt = ">B{}s".format(len(buffer) - 1)
        try:
            self.gwid, self.gwadd = struct.unpack(fmt, buffer)
            return True
        except struct.error:
            return False


class MQTTSNMessageConnect(MQTTSNMessage):
    def __init__(self, duration=30):
        super().__init__()
        self.flags = MQTTSNFlags()
        self.protocol_id = 0x01
        self.duration = duration
        self.client_id = b''

    def pack(self):
        header = MQTTSNHeader(CONNECT)
        flags_bytes = self.flags.pack()
        self.client_id = self.client_id[:MQTTSN_MAX_CLIENTID_LEN]
        msg = header.pack(1 + 1 + 2 + len(self.client_id))
        msg += struct.pack(">cBH{}s".format(len(self.client_id)), flags_bytes,
                           self.protocol_id, self.duration, self.client_id)
        return msg

    def unpack(self, buffer):
        fmt = ">cBH{}s".format(len(buffer) - (1 + 1 + 2))
        try:
            flags_bytes, self.protocol_id, self.duration, self.client_id = struct.unpack(fmt, buffer)
            self.flags = MQTTSNFlags()
            self.flags.unpack(flags_bytes)
            return True if self.protocol_id == 0x01 else False
        except struct.error:
            return False


class MQTTSNMessageConnack(MQTTSNMessage):
    def __init__(self, return_code=MQTTSN_RC_ACCEPTED):
        super().__init__()
        self.return_code = return_code

    def pack(self):
        header = MQTTSNHeader(CONNACK)
        msg = header.pack(1)
        msg += struct.pack(">B", self.return_code)
        return msg

    def unpack(self, buffer):
        try:
            self.return_code = struct.unpack(">B", buffer)[0]
            return True
        except struct.error:
            return False


class MQTTSNMessageRegister(MQTTSNMessage):
    def __init__(self, topic_id=0x0000):
        super().__init__()
        self.topic_id = topic_id
        self.msg_id = 0
        self.topic_name = b''

    def pack(self):
        header = MQTTSNHeader(REGISTER)

        fixed_len = MQTTSN_HEADER_LEN + 2 + 2
        self.topic_name = self.topic_name[:MQTTSN_MAX_MSG_LEN - fixed_len]

        msg = header.pack(2 + 2 + len(self.topic_name))
        msg += struct.pack(">HH{}s".format(len(self.topic_name)), self.topic_id,
                           self.msg_id, self.topic_name)
        return msg

    def unpack(self, buffer):
        fmt = ">HH{}s".format(len(buffer) - (2 + 2))
        try:
            self.topic_id, self.msg_id, self.topic_name = struct.unpack(fmt, buffer)
            return True
        except struct.error:
            return False


class MQTTSNMessageRegack(MQTTSNMessage):
    def __init__(self, return_code=MQTTSN_RC_ACCEPTED):
        super().__init__()
        self.topic_id = 0
        self.msg_id = 0
        self.return_code = return_code

    def pack(self):
        header = MQTTSNHeader(REGACK)
        msg = header.pack(2 + 2 + 1)
        msg += struct.pack(">HHB", self.topic_id, self.msg_id, self.return_code)
        return msg

    def unpack(self, buffer):
        fmt = ">HHB"
        try:
            self.topic_id, self.msg_id, self.return_code = struct.unpack(fmt, buffer)
            return True
        except struct.error:
            return False


class MQTTSNMessagePublish(MQTTSNMessage):
    def __init__(self, msg_id=0x0000):
        super().__init__()
        self.flags = MQTTSNFlags()
        self.topic_id = 0
        self.msg_id = msg_id
        self.data = b''

    def pack(self):
        header = MQTTSNHeader(PUBLISH)

        fixed_len = MQTTSN_HEADER_LEN + 1 + 2 + 2
        flags_bytes = self.flags.pack()
        self.data = self.data[:MQTTSN_MAX_MSG_LEN-fixed_len]

        msg = header.pack(fixed_len + len(self.data))
        msg += struct.pack(">cHH{}s".format(len(self.data)), flags_bytes,
                           self.topic_id, self.msg_id, self.data)
        return msg

    def unpack(self, buffer):
        fixed_len = 1 + 2 + 2
        fmt = ">cHH{}s".format(len(buffer) - fixed_len)
        try:
            flags_bytes, self.topic_id, self.msg_id, self.data = struct.unpack(fmt, buffer)
            self.flags = MQTTSNFlags()
            self.flags.unpack(flags_bytes)
            return True
        except struct.error:
            return False


class MQTTSNMessagePuback(MQTTSNMessage):
    def __init__(self, return_code=0x00):
        super().__init__()
        self.topic_id = 0
        self.msg_id = 0
        self.return_code = return_code

    def pack(self):
        header = MQTTSNHeader(PUBACK)
        msg = header.pack(2 + 2 + 1)
        msg += struct.pack(">HHB", self.topic_id, self.msg_id, self.return_code)
        return msg

    def unpack(self, buffer):
        fmt = ">HHB"
        try:
            self.topic_id, self.msg_id, self.return_code = struct.unpack(fmt, buffer)
            return True
        except struct.error:
            return False


# no pre-defined IDs supported for now
# only regular topic names and short topic names
# this enables the parameter to be a simple byte string
class MQTTSNMessageSubscribe(MQTTSNMessage):
    def __init__(self):
        super().__init__()
        self.flags = MQTTSNFlags()
        self.topic_id_name = b''
        self.msg_id = 0

    def pack(self):
        header = MQTTSNHeader(SUBSCRIBE)

        fixed_len = MQTTSN_HEADER_LEN + 1 + 2
        flags_bytes = self.flags.pack()
        self.topic_id_name = self.topic_id_name[:MQTTSN_MAX_MSG_LEN-fixed_len]

        msg = header.pack(fixed_len + len(self.topic_id_name))
        msg += struct.pack(">cH{}s".format(len(self.topic_id_name)), flags_bytes,
                           self.msg_id, self.topic_id_name)
        return msg

    def unpack(self, buffer):
        fixed_len = 1 + 2
        fmt = ">cH{}s".format(len(buffer) - fixed_len)
        try:
            flags_bytes, self.msg_id, self.topic_id_name = struct.unpack(fmt, buffer)
            self.flags = MQTTSNFlags()
            self.flags.unpack(flags_bytes)
            return True
        except struct.error:
            return False


class MQTTSNMessageSuback(MQTTSNMessage):
    def __init__(self, return_code=MQTTSN_RC_ACCEPTED):
        super().__init__()
        self.flags = MQTTSNFlags()
        self.topic_id = 0
        self.msg_id = 0
        self.return_code = return_code

    def pack(self):
        header = MQTTSNHeader(SUBACK)
        msg = header.pack(1 + 2 + 2 + 1)

        flags_bytes = self.flags.pack()
        msg += struct.pack(">cHHB", flags_bytes, self.topic_id, self.msg_id, self.return_code)
        return msg

    def unpack(self, buffer):
        fmt = ">cHHB"
        try:
            flags_bytes, self.topic_id, self.msg_id, self.return_code = struct.unpack(fmt, buffer)
            self.flags = MQTTSNFlags()
            self.flags.unpack(flags_bytes)
            return True
        except struct.error:
            return False


class MQTTSNMessageUnsubscribe(MQTTSNMessage):
    def __init__(self):
        super().__init__()
        self.flags = MQTTSNFlags()
        self.topic_id_name = b''
        self.msg_id = 0

    def pack(self):
        header = MQTTSNHeader(UNSUBSCRIBE)

        fixed_len = MQTTSN_HEADER_LEN + 1 + 2
        flags_bytes = self.flags.pack()
        self.topic_id_name = self.topic_id_name[:MQTTSN_MAX_MSG_LEN-fixed_len]

        msg = header.pack(fixed_len + len(self.topic_id_name))
        msg += struct.pack(">cH{}s".format(len(self.topic_id_name)), flags_bytes,
                           self.msg_id, self.topic_id_name)
        return msg

    def unpack(self, buffer):
        fixed_len = 1 + 2
        fmt = ">cH{}s".format(len(buffer) - fixed_len)
        try:
            flags_bytes, self.msg_id, self.topic_id_name = struct.unpack(fmt, buffer)
            self.flags = MQTTSNFlags()
            self.flags.unpack(flags_bytes)
            return True
        except struct.error:
            return False


class MQTTSNMessageUnsuback(MQTTSNMessage):
    def __init__(self):
        super().__init__()
        self.msg_id = 0

    def pack(self):
        header = MQTTSNHeader(UNSUBACK)
        msg = header.pack(2)
        msg += struct.pack(">H", self.msg_id)
        return msg

    def unpack(self, buffer):
        fmt = ">H"
        try:
            self.msg_id = struct.unpack(fmt, buffer)[0]
            return True
        except struct.error:
            return False


class MQTTSNMessagePingreq(MQTTSNMessage):
    def __init__(self):
        super().__init__()
        self.client_id = b''

    def pack(self):
        header = MQTTSNHeader(PINGREQ)
        self.client_id = self.client_id[:MQTTSN_MAX_CLIENTID_LEN]
        msg = header.pack(len(self.client_id))
        msg += self.client_id
        return msg

    def unpack(self, buffer):
        self.client_id = buffer
        return True


class MQTTSNMessagePingresp(MQTTSNMessage):
    def __init__(self):
        super().__init__()

    def pack(self):
        header = MQTTSNHeader(PINGRESP)
        msg = header.pack()
        return msg

    def unpack(self, buffer):
        return True


class MQTTSNMessageDisconnect(MQTTSNMessage):
    def __init__(self):
        super().__init__()
        self.duration = 0

    def pack(self):
        header = MQTTSNHeader(DISCONNECT)

        # disconnect with duration
        if self.duration:
            msg = header.pack(2)
            msg += struct.pack(">H", self.duration)
        else:
            msg = header.pack()
        return msg

    def unpack(self, buffer):
        # regular disconnect
        if not buffer:
            return True

        # disconnect with duration
        fmt = ">H"
        try:
            self.duration = struct.unpack(fmt, buffer)
            return True
        except struct.error:
            return False


if __name__ == "__main__":
    test_objs = [MQTTSNMessageAdvertise, MQTTSNMessageConnack, MQTTSNMessageConnect,
                 MQTTSNMessageDisconnect, MQTTSNMessageGWInfo, MQTTSNMessagePingreq,
                 MQTTSNMessagePingresp, MQTTSNMessagePuback, MQTTSNMessagePublish,
                 MQTTSNMessageRegack, MQTTSNMessageRegister, MQTTSNMessageSearchGW,
                 MQTTSNMessageSuback, MQTTSNMessageSubscribe, MQTTSNMessageUnsuback,
                 MQTTSNMessageUnsubscribe]

    for obj in test_objs:
        obj1 = obj()
        obj2 = obj()
        obj3 = obj()
        packet = obj1.pack()
        head = MQTTSNHeader()
        ret = head.unpack(packet)
        if packet[0] != head.length + MQTTSN_HEADER_LEN or packet[1] != head.msg_type:
            print(obj1.__class__.__name__)
            print("Header error: ", packet)
            break
        obj2.unpack(packet[ret:])
        if obj1 != obj2:
            print(obj1.__class__.__name__)
            print("Error:", obj1)
            print("\t", obj3)
            break
        else:
            print("OK:", obj1.__class__.__name__)
