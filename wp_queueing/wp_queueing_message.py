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
import wp_queueing.wp_queueing_base as wp_queueing_base


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



class QueueMessage:
    """ Serializable objects for sending information to or retrieving information from message queues.

    Attributes:
        _msg_id : str
            Unique identifier of a queue message object.
        _msg_dt : str
            Timestamp when the message was created.
        _msg_topic : str
            MQTT topic for the queue message.
        _payload : dict
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
        self._msg_id = str(uuid.uuid4())
        self._msg_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self._msg_topic = msg_topic
        self._payload = dict()

    def __str__(self) -> str:
        """ Converts the object into a string.

        Returns:
            str : textual representation of the object.
        """
        temp_dict = {
            'topic': self._msg_topic,
            'payload': self.json_serialize_payload()
        }
        return str(temp_dict)

    @property
    def msg_id(self) -> str:
        """ Getter for the unique message identifier. """
        return self._msg_id

    @property
    def msg_timestamp(self) -> str:
        """ Getter for the timestamp when the message was created. """
        return self._msg_dt

    @property
    def msg_topic(self) -> str:
        """ Getter for the MQTT topic. """
        return self._msg_topic

    @property
    def msg_payload(self) -> dict:
        """ Getter for the message payload. """
        return self._payload

    @msg_payload.setter
    def msg_payload(self, payload) -> None:
        """ Setter for the message payload. Value can either be a dictionary or an object implementing IConvertToDict.

        Parameters:
            payload : dict or IConvertToDict
                Payload of the message object.
        """
        if isinstance(payload, type(dict)):
            self._payload = payload
        elif issubclass(type(payload), IConvertToDict):
            self._payload = payload.to_dict()
        else:
            raise wp_queueing_base.QueueingException(
                'InvalidPayloadType',
                'QueueMessage.payload.setter: invalid payload type ("{}")'.format(type(payload)))

    def json_serialize_payload(self) -> str:
        """ Converts the message objects into a JSON string.

        Returns:
            str : message object converted to a JSON string.
        """
        temp_dict = {
            'msg_id': self._msg_id,
            'msg_dt': self._msg_dt,
            'payload': self._payload
        }
        return json.dumps(temp_dict)

    def json_deserialize_payload(self, json_str) -> None:
        """ Converts a JSON string into a message object.

        Parameters:
            json_str : str
                JSON string containing a serialized message object.
        """
        temp_obj = json.loads(json_str)
        self._msg_id = temp_obj['msg_id']
        self._msg_dt = temp_obj['msg_dt']
        self._payload = temp_obj['payload']

    @property
    def mqtt_message(self) -> str:
        """ Getter that creates a well-formatted MQTT message out of the message object. """
        return self.json_serialize_payload()

    @mqtt_message.setter
    def mqtt_message(self, message) -> None:
        """ Setter that converts a MQTT message retrieved from a message queue into a message object.

        Parameters:
            message : dict
                Message read from a MQTT message queue.
        """
        self._msg_topic = message['topic']
        self.json_deserialize_payload(message['payload'])
