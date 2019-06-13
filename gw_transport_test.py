from mqttsn_transport_udp import MQTTSNTransportUDP


own_port = 10000
gw = MQTTSNTransportUDP(own_port)
print("Starting server.")
while True:
    try:
        read, addr = gw.read_packet()
        if not read:
            continue
        print("Recvd: ", read.decode(), "from", addr)
        gw.write_packet(b"Echo: " + read, addr)
    except KeyboardInterrupt:
        break
