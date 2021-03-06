from mqttsn_transport import MQTTSNTransport
from mqttsn_defines import MQTTSN_MAX_MSG_LEN
import socket

# to do: listen on broadcast


class MQTTSNTransportUDP(MQTTSNTransport):
    def __init__(self, _port, own_addr):
        super().__init__()

        # Create a TCP/IP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setblocking(False)

        # Bind the socket to the port
        self.own_addr = own_addr
        self.to_addr = ('<broadcast>', _port)
        self.sock.bind(('', _port))

    def read_packet(self):
        try:
            data, address = self.sock.recvfrom(MQTTSN_MAX_MSG_LEN)
        except OSError:
            return b'', None

        # make sure its for us or a broadcast, and that we didnt send it either
        if data[1:2] in (self.own_addr, b'\xff') and data[0:1] != self.own_addr:
            return data[2:], data[0:1]
        return b'', None

    def write_packet(self, data, dest):
        # from + to + data
        data = self.own_addr + dest + data
        self.sock.sendto(data, self.to_addr)
        return len(data)

    def broadcast(self, data):
        # from + to + data
        data = self.own_addr + b'\xff' + data
        self.sock.sendto(data, self.to_addr)
        return len(data)

    def end(self):
        self.sock.close()


if __name__ == '__main__':
    gw_addr = b'\x01'

    port = 20000
    clnt = MQTTSNTransportUDP(port, b'\x02')
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
            clnt.end()
            break
