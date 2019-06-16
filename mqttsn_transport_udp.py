from mqttsn_transport import MQTTSNTransport, MQTTSNAddress
from mqttsn_defines import MQTTSN_MAX_MSG_LEN
import socket

# to do: listen on broadcast


class MQTTSNTransportUDP(MQTTSNTransport):
    def __init__(self, port, local_addr):
        super().__init__()

        # Create a TCP/IP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setblocking(False)

        # Bind the socket to the port
        self.local = local_addr
        self.to_addr = ('<broadcast>', port)
        self.sock.bind(('', port))

    def read_packet(self):
        try:
            data, address = self.sock.recvfrom(MQTTSN_MAX_MSG_LEN)
        except OSError:
            return b'', None

        # make sure its for us or a broadcast, and that we didnt send it either
        if data[1:2] in (self.local, b'\xff') and data[0:1] != self.local:
            return data[2:], MQTTSNAddress(data[0:1])
        return b'', None

    def write_packet(self, data, dest):
        data = self.local + dest.bytes + data
        self.sock.sendto(data, self.to_addr)
        # from + to + data
        return len(data)

    def broadcast(self, data):
        data = self.local + b'\xff' + data
        self.sock.sendto(data, self.to_addr)
        return len(data)


if __name__ == '__main__':
    gw_addr = MQTTSNAddress(b'\x01')

    own_port = 20000
    clnt = MQTTSNTransportUDP(own_port, b'\x02')
    print("Starting client.")
    import time

    while True:
        try:
            time.sleep(1)
            clnt.broadcast(b"Hello world")
            while True:
                read, addr = clnt.read_packet()
                if read:
                    print("Recvd: ", read.decode(), "from", addr.bytes)
                    break
        except KeyboardInterrupt:
            break
