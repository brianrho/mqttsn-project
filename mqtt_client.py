import abc


# MQTT client interface, with methods relevant to the gateway
class MQTTClient:
    @abc.abstractmethod
    def register_handlers(self, conn_disconn_cb, msg_cb):
        pass

    @abc.abstractmethod
    def publish(self, topic, data, qos=0, retain=0):
        pass

    @abc.abstractmethod
    def subscribe(self, topic, qos=0):
        pass

    @abc.abstractmethod
    def unsubscribe(self, topic):
        pass
