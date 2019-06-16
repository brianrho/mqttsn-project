from mqttsn_transport_udp import MQTTSNTransportUDP
import time

own_port = 20000
gw = MQTTSNTransportUDP(own_port, b'\x01')

print("Starting server.")
while True:
    try:
        time.sleep(1)
        read, addr = gw.read_packet()
        if not read:
            continue
        print("Recvd: ", read.decode(), "from", addr.bytes)
        gw.broadcast(b"Echo: " + read)
    except KeyboardInterrupt:
        gw.end()
        break
