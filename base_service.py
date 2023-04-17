import paho.mqtt.client as mqtt
from abc import ABCMeta, abstractmethod
import yaml


def broker_config():
    with open('broker_config.yaml', 'r', encoding='utf-8') as f:
        return yaml.load(f.read(), Loader=yaml.FullLoader)["mqtt_broker"]


class base_service(metaclass=ABCMeta):
    def __init__(self, mqtt_broker, sub_topics_array, topic_func):
        self.client = mqtt.Client()
        self.mqtt_broker = mqtt_broker
        self.sub_topics_array = sub_topics_array
        self.topic_func = topic_func

    @abstractmethod
    def on_connect(self, client, userdata, flags, rc):
        pass

    @abstractmethod
    def on_subscribe(self, client, userdata, mid, granted_qos):
        pass

    @abstractmethod
    def on_publish(self, topic, payload, qos):
        pass

    @abstractmethod
    def on_msg(self, client, userdata, msg):
        pass
    
    @abstractmethod
    def on_message(self, client, userdata, msg):
        pass
    
    def on_pre_connect(self, *args):
        """This seems to be needed due to a bug in paho-1-6-1"""
        """self.logger.debug('running on_pre_connect, apparently paho needs this now')"""
        pass

    def connect(self):
        self.client.on_connect = self.on_connect
        self.client.on_pre_connect = self.on_pre_connect
        self.client.connect(self.mqtt_broker, port=1883, keepalive=60)

    def sub_topics(self):
        self.client.subscribe(self.sub_topics_array)
        self.client.on_message = self.on_msg
        self.client.on_subscribe = self.on_subscribe

    def sub_topics_func(self, array):
        self.client.subscribe(array)
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe

    def pub_topics(self, topic, payload, qos):
        self.client.publish(topic, payload, qos)
        self.client.on_publish = self.on_publish

    def start_loop(self):
        self.client.loop_forever()
