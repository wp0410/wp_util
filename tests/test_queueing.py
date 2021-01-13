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
import unittest
import uuid
from datetime import datetime
import wp_queueing as wpq

class TMessageOK(wpq.IConvertToDict):
    def __init__(self):
        super().__init__()

    def to_dict(self) -> dict:
        temp_dict = super().to_dict()
        temp_dict['class'] = 'TMessage1'
        return temp_dict

class TMessageFail:
    def __init__(self):
        self.t = 1

class Test1QueueMessage(unittest.TestCase):
    def __init_config(self):
        self.__config = {
            "host": "localhost"
        }

    def setUp(self):
        super().setUp()

    def test_01(self):
        print("")
        msg_1 = wpq.QueueMessage(msg_topic = "a_topic")
        self.assertIsNotNone(msg_1)
        print(str(msg_1))
        self.assertTrue(type(msg_1.msg_id) is str and len(msg_1.msg_id) == len(str(uuid.uuid4())))
        self.assertIsNotNone(msg_1.msg_topic)
        self.assertEqual(msg_1.msg_topic, "a_topic")
        self.assertTrue(type(msg_1.msg_timestamp) is str)
        try:
            temp_dt = datetime.strptime(msg_1.msg_timestamp, "%Y-%m-%d %H:%M:%S.%f")
        except:
            self.assertTrue(False, "Invalid datetime format")
        self.assertTrue(type(msg_1.msg_payload) is dict and len(msg_1.msg_payload) == 0)
        with self.assertRaises(wpq.QueueingException):
            msg_1.msg_payload = TMessageFail()
        try:
            msg_1.msg_payload = TMessageOK()
        except wpq.QueueingException:
            self.assertTrue(False, 'Invalid Payload Type')
        self.assertIsNotNone(msg_1.msg_payload)
        self.assertTrue(type(msg_1.msg_payload) is dict and 'class' in msg_1.msg_payload)
        msg_2 = wpq.QueueMessage()
        self.assertIsNotNone(msg_2)
        temp_mqtt = msg_1.mqtt_message
        self.assertEqual(temp_mqtt['topic'], msg_1.msg_topic)
        msg_2.mqtt_message = temp_mqtt
        self.assertEqual(msg_1.msg_id, msg_2.msg_id)
        self.assertEqual(msg_1.msg_timestamp, msg_2.msg_timestamp)
        self.assertEqual(msg_1.msg_topic, msg_2.msg_topic)
        self.assertIsNotNone(msg_2.msg_payload)
        self.assertTrue(type(msg_2.msg_payload) is dict and 'class' in msg_2.msg_payload)
        self.assertEqual(msg_1.msg_payload['class'], msg_2.msg_payload['class'])

if __name__ == '__main__':
    unittest.main(verbosity=5)