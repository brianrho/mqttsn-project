import abc


class MQTTSNTransport(abc.ABC):
    @abc.abstractmethod
    def read_packet(self):
        return b'', None

    @abc.abstractmethod
    def write_packet(self, data: bytes, dest: bytes):
        return 0

    @abc.abstractmethod
    def broadcast(self, data: bytes):
        return 0
