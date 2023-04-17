import json
import base_service
import yaml


MQTT_MSG = json.dumps({
          "ApiVersion": "v2",
          "ContentType": "application/json",
          "CorrelationID": "14a42ea6-c394-41c3-8bcd-a29b9f5e6835",
          "RequestId": "e6e8a2f4-eb14-4649-9e2b-175247911369",
          "QueryParams": {
            "ds-pushevent": "no",
            "ds-returnevent": "yes"
          }
        })


class edgex_service(base_service.base_service):
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_publish(self, topic, payload, qos):
        print("Publish: " + str(topic) + " " + str(payload))

    def on_message(self, client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    def on_msg(self, client, userdata, msg):
        # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        for topic in self.topic_func:
            if topic[0] == msg.topic:
                # 调用主题对应的功能函数
                getattr(func_class, topic[1])(self)

class func_class(base_service.base_service):
    # 发送主题
    def topic_a_func(self):
        edgex_service.pub_topics(self, "edgex/command/request/WJ03-C02-Screwer-01/ReadAllProperties/get", MQTT_MSG, 1)
        edgex_service.sub_topics_func(self, "edgex/command/response/#")

    def topic_b_func(self):
        print("topic_b_func()函数暂时没有功能实现！")

def run():
    with open('edgex_service_config.yaml', 'r', encoding='utf-8') as f:
        result = yaml.load(f.read(), Loader=yaml.FullLoader)
    client = edgex_service(base_service.broker_config(), result["sub_topics_array"], result["topic_func"])
    client.connect()
    client.sub_topics()
    client.start_loop()


if __name__ == '__main__':
    run()
