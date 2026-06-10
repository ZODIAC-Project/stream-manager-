import logging
import time

class PurposeSubscribeClient:
    """
    Minimal purpose-aware MQTT subscriber wrapper.

    This class contains only the subscription-related behavior extracted from
    `purpose_client.py`, including purpose-aware subscribe logic and pending
    subscription tracking.
    """

    AP = "!AP"
    PRESUB = "!PBAC/PRESUB"

    def __init__(self, client, log_problems=True, purpose_aware=False, qos=0, presub=False) -> None:
        self.client = client
        self.logger = logging.getLogger(__name__)
        self.purpose_aware = purpose_aware
        self.presub = presub
        self.qos = qos
        self.subscriptions_pending = []

        self.client.on_connect = self.on_connect
        if log_problems:
            self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_subscribe = self._on_subscribe_manage_pending

        self.loop_start = self.client.loop_start
        self.loop_stop = self.client.loop_stop
        self.loop_forever = self.client.loop_forever
        self.message_callback_add = self.client.message_callback_add

    def _on_subscribe_manage_pending(self, client, userdata, mid, granted_qos, properties=None):
        self.logger.debug("ack for subscription %s", mid)
        try:
            self.subscriptions_pending.remove(mid)
        except ValueError:
            pass

    def wait_for_subscriptions(self):
        TIMEOUT = 30
        TICK = 0.01
        timeout = TIMEOUT

        self.logger.debug("waiting for subscriptions...")

        while timeout > 0 and len(self.subscriptions_pending) > 0:
            time.sleep(TICK)
            timeout -= TICK

        if timeout <= 0:
            self.logger.warning("subscription wait timer timed out!")

    def subscribe(self, topic, qos=1):
        (success, mid) = self.client.subscribe(topic, qos=qos)
        self.logger.debug("subscribed with mid %s, success: %s", mid, success)
        if qos > 0:
            self.subscriptions_pending.append(mid)

    @staticmethod
    def escape_topic(topic):
        return topic.replace("#", "HASH").replace("+", "PLUS")

    def subscribe_with_purpose(self, topic: str, ap: str, qos=0, presub=False):
        topic = self.escape_topic(topic)
        if self.presub or presub:
            cid = self.client._client_id.decode("utf-8")
            mi = self.client.publish(self.PRESUB + "/%s/%s{%s}" % (cid, topic, ap), "", qos=1)
            mi.wait_for_publish()
            return self.subscribe(topic, qos=qos)
        else:
            purpose_topic = (self.AP + "/%s{%s}" % (topic, ap))
            return self.subscribe(purpose_topic, qos=qos)

    def on_connect(self, client, userdata, flags, rc, properties=None):
        try:
            self.logger.debug("Connected with result code %s properties=%s", rc, getattr(properties, "__dict__", properties))
        except Exception:
            self.logger.debug("Connected with result code %s", rc)

    def on_message(self, client, userdata, msg):
        self.logger.debug("received message on %s", msg.topic)

    def on_disconnect(self, client, userdata, rc, properties=None):
        try:
            self.logger.critical("disconnect! rc=%s properties=%s", rc, getattr(properties, "__dict__", properties))
        except Exception:
            self.logger.critical("disconnect! (failed to format properties)")
