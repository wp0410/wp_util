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
import uuid
import json
from datetime import datetime
import paho.mqtt.client as mqtt

class IConvertToDict:
    """ Interface class: objects can be converted to a dictionary.

    Methods:
        to_dict : dict
            Converts the object to a dictionary
    """
    def to_dict(self) -> dict:
        """ Converts the object to a dictionary

        Returns:
            dict : Object converted to a dictionary
        """
        # pylint: disable=no-self-use
        return dict()



class QueueingException(Exception):
    """ Exception thrown by the queueing utilities.

    Attributes:
        reason : str
            Reason for throwing the exception.
        message : str
            Error message

    Methods:
        __str__ : str
            Converts the exception object to a string.
    """
    def __init__(self, reason, message):
        """ Contructor """
        super().__init__()
        self.reason = reason
        self.message = message

    def __str__(self) -> str:
        """ Converts the exception object to a string.

        Returns:
            str : String representation of the exception object.
        """
        return 'QUEUEING EXCEPTION({}): {}'.format(self.reason, self.message)



class QueueMessage:
    """ Serializable objects for sending information to or retrieving information from message queues.

    Attributes:
        __msg_id : str
            Unique identifier of a queue message object.
        __msg_dt : str
            Timestamp when the message was created.
        __msg_topic : str
            MQTT topic for the queue message.
        __payload : dict
            Dictionary containing the message payload.

    Properties:
        msg_id : get, str
            Getter for the unique message identifier.
        msg_timestamp : get, str
            Getter for the timestamp when the message was created.
        msg_topic : get, str
            Getter for the MQTT topic.
        payload : get, dict
            Getter for the message payload.
        payload : set, dict or IConvertToDict
            Setter for the message payload. Value can either be a dictionary or an object implementing IConvertToDict.
        mqtt_message : get, dict
            Getter that creates a well-formatted MQTT message out of the message object.
        mqtt_message : set, dict
            Setter that converts a MQTT message retrieved from a message queue into a message object.
    Methods:
        QueueMessage : None
            Constructor.
        __str__ : str
            Converts the object into a string.
        json_serialize_payload : str
            Converts the message objects into a JSON string.
        json_deserialize_payload : None
            Converts a JSON string into a message object.
    """
    def __init__(self, msg_topic = None):
        """ Constructor.

        Parameters:
            msg_topic : str
                MQTT topic for publishing the message.
        """
        self.__msg_id = str(uuid.uuid4())
        self.__msg_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.__msg_topic = msg_topic
        self.__payload = dict()

    def __str__(self) -> str:
        """ Converts the object into a string.

        Returns:
            str : textual representation of the object.
        """
        temp_dict = {
            'topic': self.__msg_topic,
            'payload': self.json_serialize_payload()
        }
        return str(temp_dict)

    @property
    def msg_id(self) -> str:
        """ Getter for the unique message identifier. """
        return self.__msg_id

    @property
    def msg_timestamp(self) -> str:
        """ Getter for the timestamp when the message was created. """
        return self.__msg_dt

    @property
    def msg_topic(self) -> str:
        """ Getter for the MQTT topic. """
        return self.__msg_topic

    @property
    def msg_payload(self) -> dict:
        """ Getter for the message payload. """
        return self.__payload

    @msg_payload.setter
    def msg_payload(self, payload) -> None:
        """ Setter for the message payload. Value can either be a dictionary or an object implementing IConvertToDict.

        Parameters:
            payload : dict or IConvertToDict
                Payload of the message object.
        """
        if payload is type(dict):
            self.__payload = payload
        elif issubclass(type(payload), IConvertToDict):
            self.__payload = payload.to_dict()
        else:
            raise QueueingException(
                'InvalidPayloadType',
                'QueueMessage.payload.setter: invalid payload type ("{}")'.format(type(payload)))

    def json_serialize_payload(self) -> str:
        """ Converts the message objects into a JSON string.

        Returns:
            str : message object converted to a JSON string.
        """
        temp_dict = {
            'msg_id': self.__msg_id,
            'msg_dt': self.__msg_dt,
            'payload': self.__payload
        }
        return json.dumps(temp_dict)

    def json_deserialize_payload(self, json_str) -> None:
        """ Converts a JSON string into a message object.

        Parameters:
            json_str : str
                JSON string containing a serialized message object.
        """
        temp_obj = json.loads(json_str)
        self.__msg_id = temp_obj['msg_id']
        self.__msg_dt = temp_obj['msg_dt']
        self.__payload = temp_obj['payload']

    @property
    def mqtt_message(self) -> dict:
        """ Getter that creates a well-formatted MQTT message out of the message object. """
        return {
            'topic': self.__msg_topic,
            'payload': self.json_serialize_payload(),
            'qos': 0
        }

    @mqtt_message.setter
    def mqtt_message(self, message) -> None:
        """ Setter that converts a MQTT message retrieved from a message queue into a message object.

        Parameters:
            message : dict
                Message read from a MQTT message queue.
        """
        self.__msg_topic = message['topic']
        self.json_deserialize_payload(message['payload'])



class QueueingConfig:
    """ Class holding the configuration of the queueing system.

    Attributes:
        __config_dict : dict
            Dictionary holding the configuration settings (name-value pairs).
        __is_valid : bool
            Indicator whether or not the configuration is valid.

    Methods:
        QueueConfig()
            Constructor
        __getitem__
            Accessor for the dictionary elements.
    """
    def __init__(self, config_dict: dict):
        """ Constructor.

        Parameters:
            config_dict : dict
                Dictionary containing the configuration as name-value pairs.
        """
        if not config_dict is dict:
            raise QueueingException(
                'InvalidConfiguration', 'Configuration must be a dictionary')
        self.__config_dict = dict(config_dict)
        exception_detail = 'Missing configuration element: "host"'
        self.__is_valid = 'host' in self.__config_dict
        if 'port' not in self.__config_dict:
            self.__config_dict['port'] = 1883
        if 'qos' not in self.__config_dict:
            self.__config_dict['qos'] = 0
        if self.__is_valid:
            exception_detail = 'Invalid configuration element: "qos". Value is {}'.format(self.__config_dict['qos'])
            self.__is_valid = self.__config_dict['qos'] in [0, 1, 2]
        if not self.__is_valid:
            raise QueueingException('InvalidConfiguration', exception_detail)

    def __getitem__(self, config_key):
        """ Accessor for the dictionary elements.

        Parameters:
            config_key : str
                Name of a configuration parameter.

        Returns:
            Any : Value of the configuration parameter.
        """
        if config_key in self.__config_dict:
            return self.__config_dict[config_key]
        raise QueueingException(
            'InvalidConfiguration', 'Undefined configuration element: "{}"'.format(config_key))



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
        userdata.is_connected = True
    else:
        userdata.is_connected = False
        fault_txt = ['', 'PROTOCOL_VERSION', 'IDENTIFIER_REJECTED', 'SERVER_UNAVAILABLE']
        raise QueueingException(
            'MQTTConnectionRejected', 'MQTT Connection failed; reason: "{}"'.format(fault_txt[rc]))



def mqtt_on_publish(client, userdata, mid):
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
    userdata.messages_published += 1



class MQTTClient:
    """ Base class for MQTT clients

    Attributes:
        is_connected : boolean
            Indication whether or not the broker connection is successfully established.
        logger : logging.Logger
            Logger to be used for logging.
        config : QueueingConfig
            Configuration settings.
        mqtt_client : mqtt.Client
            MQTT client holding the connection to the MQTT broker.

    Methods:
        MQTTClient
            Constructor
        __del__
            Destructor.
    """
    def __init__(self, config, logger, client_id = ""):
        """ Constructor.

        Parameters:
            config : dict
                Dictionary containing the required configuration parameters as name-value pairs.
            logger : logging.Logger
                Logger to be used for logging.
            client_id : str, optional
                Identifier of the MQTT client. If empty, a random identifier will be generated.
        """
        self.is_connected = False
        self.logger = logger
        self.config = QueueingConfig(config)
        self.mqtt_client = mqtt.Client(client_id = client_id, clean_session = False, userdata = self)
        self.mqtt_client.on_connect = mqtt_on_connect
        self.mqtt_client.enable_logger(self.logger)
        self.mqtt_client.connect(self.config['host'], self.config['port'])

    def __del__(self):
        """ Destructor. """
        if self.is_connected:
            self.mqtt_client.disconnect()
        self.mqtt_client = None
        self.config = None



class MQTTProducer(MQTTClient):
    """ MQTT client to publish messages to an MQTT broker.

    Attributes:
        __last_rc : list
            Result of the most recent publish operation.

    Methods:
        MQTTProducer
            Constructor.
        __publish
            Sends an message to the MQTT broker.
        publish_single
            Sends a single message to the MQTT broker.
        publish_many
            Sends a list of messages to the MQTT broker.
    """
    def __init__(self, config, logger, client_id = ""):
        """ Constructor.

        Parameters:
            config : dict
                Dictionary containing the required configuration parameters as name-value pairs.
            logger : logging.Logger
                Logger to be used for logging.
            client_id : str, optional
                Identifier of the MQTT client. If empty, a random identifier will be generated.
        """
        super().__init__(config, logger, client_id)
        self.messages_published = 0
        self.__last_rc = None
        self.mqtt_client.on_publish = mqtt_on_publish

    def __publish(self, message) -> int:
        """ Sends an message to the MQTT broker.

        Paramters:
            message : QueueMessage
                Message to be sent.

        Returns:
            int : Number of messages successfully sent (0 or 1).
        """
        self.logger.debug('MQTTProducer.__publish(message_type="{}")'.format(type(message)))
        if not issubclass(type(message), QueueMessage):
            self.logger.error('MQTTProducer.__publish: InvalidMessageFormat "{}"'.format(type(message)))
            raise QueueingException(
                'InvalidMessageFormat', 'Invalid message type for sending: "{}"'.format(type(message)))
        self.__last_rc = self.mqtt_client.publish(message.msg_topic, message.mqtt_message, self.config['qos'])
        self.logger.debug('MQTTProducer.__publish: returned {}'.format(str(self.__last_rc)))
        res = self.__last_rc[0]
        if res == mqtt.MQTT_ERR_SUCCESS:
            return 1
        return 0

    def publish_single(self, message) -> int:
        """ Sends a single message to the MQTT broker.

        Paramters:
            message : QueueMessage
                Message to be sent.

        Returns:
            int : Number of messages successfully sent (0 or 1).
        """
        return self.__publish(message)

    def publish_many(self, message_list) -> int:
        """ Sends a list of messages to the MQTT broker.

        Paramters:
            message_list : array
                Messages to be sent.

        Returns:
            int : Number of messages successfully sent (0 ... len(message_list)).
        """
        num_ok = 0
        for message in message_list:
            num_ok += self.__publish(message)
        return num_ok
