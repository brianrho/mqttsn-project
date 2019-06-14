import abc


class MQTTSNAddress:
    def __init__(self, raw=''):
        self.bytes = raw


class MQTTSNTransport(abc.ABC):
    @abc.abstractmethod
    def read_packet(self):
        return b'', None

    @abc.abstractmethod
    def write_packet(self, data, dest: MQTTSNAddress):
        return 0

    @abc.abstractmethod
    def broadcast(self, data):
        return 0
