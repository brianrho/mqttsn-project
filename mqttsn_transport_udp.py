from mqttsn_transport import MQTTSNTransport, MQTTSNAddress
from mqttsn_defines import MQTTSN_MAX_MSG_LEN
import socket

# to do: listen on broadcast


class MQTTSNTransportUDP(MQTTSNTransport):
    def __init__(self, port):
        super().__init__()

        # Create a TCP/IP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setblocking(False)

        # Bind the socket to the port
        self.address = ('localhost', port)
        self.sock.bind(self.address)

    def read_packet(self):
        try:
            data, address = self.sock.recvfrom(MQTTSN_MAX_MSG_LEN)
        except OSError:
            return b'', None

        return data, MQTTSNAddress(address[1].to_bytes(2, byteorder='big'))

    def write_packet(self, data, dest):
        to_port = int.from_bytes(dest.bytes, byteorder='big')

        to_addr = ('localhost', to_port)
        self.sock.sendto(data, to_addr)
        return len(data)


if __name__ == '__main__':
    own_port = 20000
    gw_port = MQTTSNAddress(int.to_bytes(10000, 2, byteorder='big'))

    clnt = MQTTSNTransportUDP(own_port)
    print("Starting client.")
    import time

    while True:
        try:
            time.sleep(1)
            clnt.write_packet(b"Hello world", gw_port)
            while True:
                read, addr = clnt.read_packet()
                if read:
                    print("Recvd: ", read.decode(), "from", addr)
                    break
        except KeyboardInterrupt:
            break
