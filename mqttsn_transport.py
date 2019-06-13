import abc


class MQTTSNAddress:
    def __init__(self, raw=b''):
        self.bytes = raw


class MQTTSNTransport(abc.ABC):
    @abc.abstractmethod
    def read_packet(self):
        return b'', None

    @abc.abstractmethod
    def write_packet(self, data, dest: MQTTSNAddress):
        return 0
