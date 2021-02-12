"""
    Copyright 2021 Walter Pachlinger (walter.pachlinger@gmail.com)

    Licensed under the EUPL, Version 1.2 or - as soon they will be approved by the European
    Commission - subsequent versions of the EUPL (the LICENSE). You may not use this work except
    in compliance with the LICENSE. You may obtain a copy of the LICENSE at:

        https://joinup.ec.europa.eu/software/page/eupl

    Unless required by applicable law or agreed to in writing, software distributed under the
    LICENSE is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied. See the LICENSE for the specific language governing permissions
    and limitations under the LICENSE.
"""
import inspect
import logging
import paho.mqtt.client as mqtt
import wp_queueing_base
import wp_queueing_message


def mqtt_on_connect(client, userdata, flags, rc):
    """ Callback to be invoked when an attempt is made to establish a connection to the MQTT broker.

    Parameters:
        client : mqtt.Client
            Client owning the MQTT broker connection.
        userdata : MQTTProducer
            MQTTProducer instance owning the MQTT client.
        flags : Any
            Not used.
        rc : int
            Result of the attempt to connect to the MQTT broker.
    """
    # pylint: disable=unused-argument, invalid-name
    if rc == mqtt.CONNACK_ACCEPTED:
        userdata.connect_ack()
    else:
        fault_txt = ['', 'PROTOCOL_VERSION', 'IDENTIFIER_REJECTED', 'SERVER_UNAVAILABLE']
        raise wp_queueing_base.QueueingException(
            'MQTTConnectionRejected', 'MQTT Connection failed; reason: "{}"'.format(fault_txt[rc]))



class MQTTClient:
    """ Base class for MQTT clients

    Attributes:
        is_connected : boolean
            Indication whether or not the broker connection is successfully established.
        logger : logging.Logger
            Logger to be used for logging.
        mqtt_client : mqtt.Client
            MQTT client holding the connection to the MQTT broker.

    Methods:
        MQTTClient
            Constructor
        __del__
            Destructor.
        connect_ack : None
            Acknowledgement of a successful attempt to connect to a MQTT broker.
    """
    def __init__(self, broker_host: str, logger, broker_port: int = 1883):
        """ Constructor.

        Parameters:
            broker_host : str
                Name or IP address of the host where the MQTT broker is running.
            broker_port : int, optional
                Port number of the MQTT broker.
            logger : logging.Logger
                Logger to be used for logging.
        """
        mth_name = "{}.{}()".format(self.__class__.__name__, inspect.currentframe().f_code.co_name)
        self.is_connected = False
        self.logger = logger
        self.mqtt_client = mqtt.Client(userdata = self)
        self.logger.debug("{}: Created MQTT Client; client_id={}".format(mth_name, self.mqtt_client._client_id))
        self.mqtt_client.on_connect = mqtt_on_connect
        self.mqtt_client.enable_logger(self.logger)
        self.logger.debug('{}: Connecting to broker "{}:{}"'.format(mth_name, broker_host, broker_port))
        self.mqtt_client.connect(broker_host, broker_port)
        self.mqtt_client.loop(0.3, 10)

    def __del__(self):
        """ Destructor. """
        if self.is_connected:
            self.mqtt_client.disconnect()
        self.mqtt_client = None

    def connect_ack(self) -> None:
        """ Acknowledgement of a successful attempt to connect to a MQTT broker. """
        mth_name = "{}.{}()".format(self.__class__.__name__, inspect.currentframe().f_code.co_name)
        self.is_connected = True
        self.logger.debug('{}: Connection OK'.format(mth_name))



def mqtt_on_publish(client: mqtt.Client, userdata: MQTTClient, mid: int):
    """ Callback to be invoked when an attempt was made to publish a message to the MQTT broker.

    Parameters:
        client : mqtt.Client
            Client owning the MQTT broker connection.
        userdata : MQTTProducer
            MQTTProducer instance owning the MQTT client.
        mid : int
            Message identifier of the published message.
    """
    # pylint: disable=unused-argument
    userdata.publish_ack(1)



class MQTTProducer(MQTTClient):
    """ MQTT client to publish messages to an MQTT broker.

    Attributes:
        _last_rc : list
            Result of the most recent publish operation.

    Methods:
        MQTTProducer
            Constructor.
        _publish : int
            Sends an message to the MQTT broker.
        publish_single : int
            Sends a single message to the MQTT broker.
        publish_many : int
            Sends a list of messages to the MQTT broker.
        publish_ack : None
            Receives the acknowledgement for a published message.
    """
    def __init__(self, broker_host: str, logger, broker_port: int = 1883, qos: int = 0):
        """ Constructor.

        Parameters:
            broker_host : str
                Name or IP address of the host where the MQTT broker is running.
            broker_port : int, optional
                Port number of the MQTT broker.
            qos : int, optional
                Quality of service setting for the broker connection.
            logger : logging.Logger
                Logger to be used for logging.
        """
        super().__init__(broker_host=broker_host, broker_port=broker_port, logger=logger)
        self._messages_published = 0
        self._last_rc = None
        self._qos = qos
        self.mqtt_client.user_data_set(self)
        self.mqtt_client.on_publish = mqtt_on_publish

    def _publish(self, message: wp_queueing_message.QueueMessage) -> int:
        """ Sends an message to the MQTT broker.

        Paramters:
            message : QueueMessage
                Message to be sent.

        Returns:
            int : Number of messages successfully sent (0 or 1).
        """
        mth_name = "{}.{}()".format(self.__class__.__name__, inspect.currentframe().f_code.co_name)
        self.logger.debug('{}: message_type="{}")'.format(mth_name, type(message)))
        if not issubclass(type(message), wp_queueing_message.QueueMessage):
            self.logger.error('{}: InvalidMessageFormat "{}"'.format(mth_name, type(message)))
            raise wp_queueing_base.QueueingException(
                'InvalidMessageFormat', 'Invalid message type for sending: "{}"'.format(type(message)))
        self._last_rc = self.mqtt_client.publish(message.msg_topic, message.mqtt_message, self._qos)
        self.logger.debug('{}: publish request returned {}'.format(mth_name, str(self._last_rc)))
        res = self._last_rc[0]
        if res == mqtt.MQTT_ERR_SUCCESS:
            return 1
        return 0

    def publish_single(self, message: wp_queueing_message.QueueMessage) -> int:
        """ Sends a single message to the MQTT broker.

        Paramters:
            message : QueueMessage
                Message to be sent.

        Returns:
            int : Number of messages successfully sent (0 or 1).
        """
        num_ok = self._publish(message)
        self.mqtt_client.loop(timeout = 0.2)
        return num_ok


    def publish_many(self, message_list: list) -> int:
        """ Sends a list of messages to the MQTT broker.

        Paramters:
            message_list : array
                Messages to be sent.

        Returns:
            int : Number of messages successfully sent (0 ... len(message_list)).
        """
        num_ok = 0
        for message in message_list:
            num_ok += self._publish(message)
        self.mqtt_client.loop(timeout = 0.2, max_packets = len(message_list) + 1)
        return num_ok

    def publish_ack(self, num_published: int) -> None:
        """ Receives the acknowledgement for a published message.

        Parameters:
            num_published : int
                Number of acknowledged messages.
        """
        mth_name = "{}.{}()".format(self.__class__.__name__, inspect.currentframe().f_code.co_name)
        self._messages_published += num_published
        self.logger.debug('{}: publish request acknowledged'.format(mth_name))
        self.logger.debug('{}: Number of published messages: {}'.format(mth_name, self._messages_published))



class MQTTConsumer(MQTTClient):
    """ MQTT client to subscribe to and receive messages from a MQTT broker.

    Attributes:
        _subscribed_topics : list
            List of subscribed topics. Every entry must be a tuple (topic, qos).
        _owner : object
            Reference to the owner of the object. This owner class must implement method
            message(msg : wq_queueing_message.QueueMessage).
        _is_subscribed : bool
            Indicate whether or not the client is subscribed to any topics.

    Methods:
        MQTTConsumer()
            Constructor.
        __del__
            Destructor.
        subscribe_ack : None
            Acknowledge the successful subscription to a topic (or a list of topics).
        receive : None
            Poll the broker for messages to be retrieved.
        process_message : None
            Process a MQTT message received from the MQTT broker.
    """
    def __init__(self, broker_host: str, logger: logging.Logger, broker_port: int = 1883):
        """ Constructor.

        Parameters:
            broker_host : str
                Name or IP address of the host where the MQTT broker is running.
            broker_port : int, optional
                Port number of the MQTT broker.
            qos : int, optional
                Quality of service setting for the broker connection.
            logger : logging.Logger
                Logger to be used for logging.
            topics : list
                List of topics to subscribe to.
            owner : object
                Reference to the owner of the producer. The owner will be notified of new messages
                received from the broker.
        """
        super().__init__(broker_host=broker_host, broker_port=broker_port, logger=logger)
        self._owner = None
        self._is_subscribed = False
        self.mqtt_client.user_data_set(self)
        self.mqtt_client.on_message = mqtt_on_message
        self._subscribed_topics = None

    def __del__(self):
        """ Destructor. """
        if self._is_subscribed:
            self.mqtt_client.unsubscribe(self._subscribed_topics)
        super().__del__()

    @property
    def owner(self) -> object:
        """ Getter for the "owner" attribute.

        Returns:
            object : reference to the object owning the MQTTConsumer instance.
        """
        return self._owner

    @owner.setter
    def owner(self, value: object) -> None:
        """ Setter for the "owner" attribute.

        Parameters:
            value : object
                Reference to the object to own the MQTTConsumer instance.
        """
        self._owner = value

    @property
    def topics(self) -> list:
        """ Getter for the "topics" attribute containing the list of topics the MQTTConsumer is subscribed to.

        Returns:
            list : list of subscribed topics or None.
        """
        return self._subscribed_topics

    @topics.setter
    def topics(self, topic_list: list) -> None:
        """ Setter for the "topics" attribute. This setter must be used at most once,
            else a RuntimeError will be raised.

        Parameters:
            topic_list : list
                List of topics to subscribe to.
        """
        if topic_list is None:
            return
        if self._is_subscribed:
            raise RuntimeError('MQTTConsumer is already subscribed')
        self._subscribed_topics = []
        for topic in topic_list:
            if isinstance(topic, tuple):
                self._subscribed_topics.append(topic)
            else:
                self._subscribed_topics.append((topic, 0))
        self.mqtt_client.subscribe(self._subscribed_topics)
        self.receive()

    def subscribe_ack(self, mid: int, granted_qos: int) -> None:
        """ Acknowledge the successful subscription to a topic (or a list of topics).

        Parameters:
            mid : int
                Message identifier of the publish request.
            granted_qos : int
                QOS granted by the broker.
        """
        # pylint: disable=unused-argument
        self._is_subscribed = True

    def receive(self) -> None:
        """ Poll the broker for messages to be retrieved. """
        self.mqtt_client.loop(0.2, 10)

    def process_message(self, topic: str, message: str) -> None:
        """ Process a MQTT message received from the MQTT broker.

        Parameters:
            topic : str
                Topic of the received message.
            message : str
                String containing the message payload.
        """
        mth_name = "{}.{}()".format(self.__class__.__name__, inspect.currentframe().f_code.co_name)
        self.logger.debug('{}: received message:'.format(mth_name))
        self.logger.debug('       topic   = "{}"'.format(topic))
        self.logger.debug('       payload = "{}"'.format(message))
        msg = wp_queueing_message.QueueMessage()
        msg.mqtt_message = {'topic': topic, 'payload': message, 'qos': 0}
        self._owner.message(msg)

def mqtt_on_subscribe(client, userdata: MQTTConsumer, mid: int, granted_qos: int):
    """ Callback to be called when a client successfully subscribes to a topic.

    Parameters:
        client : mqtt.Client
            Client owning the MQTT broker connection.
        userdata : MQTTConsumer
            MQTTConsumer instance owning the MQTT client.
        mid : int
            Message identifier of the publish request.
        granted_qos : int
            QOS granted by the broker.
    """
    # pylint: disable=unused-argument
    userdata.subscribe_ack(mid, granted_qos)


def mqtt_on_message(client, userdata: MQTTConsumer, message: mqtt.MQTTMessage):
    """ Callback to be called when the client receives a message from the MQTT broker

    Parameters:
        client : mqtt.Client
            Client owning the MQTT broker connection.
        userdata : MQTTConsumer
            MQTTConsumer instance owning the MQTT client.
        message : mqtt.MQTTMessage
            Object containing the message topic and payload.
    """
    # pylint: disable=unused-argument
    userdata.process_message(message.topic, message.payload)
