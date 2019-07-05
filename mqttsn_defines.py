GW_ADDR_LENGTH = 2

# this is the present maximum message/packet size,
# deemed enough for our pruposes, but can be greater if needed.
# this also happens to be the minimum message/packet size to be supported by hardware
# in order to accommodate complete clientIDs and stuff
MQTTSN_MAX_MSG_LEN = 32

MQTTSN_HEADER_LEN = 2
MQTTSN_MAX_CLIENTID_LEN = 23

# in seconds
MQTTSN_DEFAULT_KEEPALIVE = 30

# used for all unicasted messages to GW
MQTTSN_T_RETRY = 5
MQTTSN_N_RETRY = 3

# in seconds
MQTTSN_T_SEARCHGW = 5

#############################
# For gateways
#############################

MQTTSN_MAX_NUM_TOPICS = 10
MQTTSN_MAX_NUM_CLIENTS = 10

MQTTSN_MAX_QUEUED_PUBLISH = 64