import gevent
from gevent.monkey import patch_all; patch_all()
from gevent.queue import Queue
from gevent.event import Event
import json
import logging
import os
import signal
import sys
import time
import uuid
import zmq.green as zmq

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Decorators
def check_reply(func):
    def wrapper(agent, msg, *args, **kwargs):
        res = func(agent, msg, *args, **kwargs)
        if msg.get('ReplyTo'):
            for to in msg.get('ReplyTo'):
                if not res:
                    res = {}
                reply = {
                    'To': to,
                    'Message': '{}Reply'.format(msg.get('Message')),
                    'ReplyToId': msg['Id']
                }
                res.update(reply) # Update reply with results from func.
                agent.tell(res)
        return res
    return wrapper



class ZActor(object):
    version = 2
    uid = None
    greenlets = []

    receive_message_count = 0
    sent_message_count = 0
    last_msg_time = time.time()
    last_msg_time_sum = 0
    ask_pool = {} # Here we keep requests that we want replies
    last_pub_sub_reconnect = None
    pub_socket = sub_socket = None


    settings = {
        'UID': False,
        'SubAddr': 'tcp://127.0.0.1:8881',
        'PubAddr': 'tcp://127.0.0.1:8882',
        'HeartbeatInterval': 120,
        'HeartbeatTimeout': 5,
        'IdleTimeout': 200,
        'Trace': False,
        'Debug': False,
        'RunMinimalMode': False,
        'CacheDir': None, # Must be set for caching.
    }

    def __init__(self, init_settings={}, override_settings={}):
        # Update startup settings
        self.settings.update(init_settings)
        #
        self.logger = self.get_logger()
        self.logger.info('Version: {}'.format(self.version))
        # Create a cache dir if required
        try:
            if self.settings.get('CacheDir') and not os.path.exists(
                    self.settings.get('CacheDir')):
                self.logger.debug('Created cache dir.')
                os.mkdir(self.settings.get('CacheDir'))
        except Exception as e:
            self.logger.error('Cannot create cache dir in {}! - {}'.format(
                self.settings.get('CacheDir'), e
            ))
        # Load live settings from cache and local
        self.load_settings()
        # Now override if given
        self.settings.update(override_settings)

        if self.settings.get('RunMinimalMode'):
            self.logger.info('Running minimal mode.')
        # Adjust logger with new settings
        self.logger.setLevel(level=logging.DEBUG if self.settings.get(
            'Debug') else logging.INFO)
        # Find my UID
        uid = self.settings.get('UID')
        if uid:
            self.uid = str(uid)
        else:
            self.uid = str(uuid.getnode())
        self.logger.info('UID: {}.'.format(self.uid))

        self.context = zmq.Context()
        self.last_pub_sub_reconnect = time.time()
        self._connect_sub_socket()
        self._connect_pub_socket()

        # gevent.spawn Greenlets
        self.greenlets.append(gevent.spawn(self.receive))
        gevent.sleep(0.1) # Give receiver time to complete connection.
        if not self.settings.get('RunMinimalMode'):
            self.greenlets.append(gevent.spawn(self.check_idle))
            self.greenlets.append(gevent.spawn(self.heartbeat))
        # Install signal handler
        gevent.signal(signal.SIGINT, self.stop)
        gevent.signal(signal.SIGTERM, self.stop)


    def _connect_pub_socket(self):
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect(self.settings.get('PubAddr'))
        self.pub_socket.setsockopt(zmq.IDENTITY, self.uid)
        self.logger.debug('Connected PUB socket.')

    def _connect_sub_socket(self):
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(self.settings.get('SubAddr'))
        self.sub_socket.setsockopt(zmq.IDENTITY, self.uid)
        # Subscribe to messages for actor and also broadcasts
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b'|{}|'.format(self.uid))
        if not self.settings.get('RunMinimalMode'):
            self.sub_socket.setsockopt(zmq.SUBSCRIBE, b'|*|')
        self.logger.debug('Connected SUB socket.')


    def _disconnect_pub_socket(self):
        self.pub_socket.setsockopt(zmq.LINGER, 0)
        self.pub_socket.close()
        self.logger.debug('Disconnected PUB socket.')

    def _disconnect_sub_socket(self):
        self.sub_socket.setsockopt(zmq.LINGER, 0)
        self.sub_socket.close()
        self.logger.debug('Disconnected SUB socket.')

    def save_settings(self):
        if self.settings.get('RunMinimalMode'):
            # Do not save settings.cache
            return
        try:
            if self.settings.get('CacheDir'):
                self.logger.debug('Saving settings.cache.')
                with open(
                    os.path.join(self.settings.get('CacheDir'),
                                'settings.cache'), 'w'
                    ) as file:
                    file.write('{}\n'.format(
                        json.dumps(self.settings, indent=4)
                    ))
        except Exception as e:
            self.logger.error('Cannot save settings.cache: {}'.format(e))


    def load_settings(self):
        try:
            if self.settings.get('CacheDir') and \
                    not self.settings.get('RunMinimalMode'):
                settings_cache = json.loads(
                    open(os.path.join(
                        self.settings.get('CacheDir'),
                        'settings.cache')
                    ).read())
                self.settings.update(settings_cache)
                self.logger.info('Loaded settings.cache.')
        except Exception as e:
            self.logger.warning('Did not import cached settings: {}'.format(e))
        # Open local settings and override settings.
        try:
            self.local_settings = json.loads(open('settings.local').read())
            self.settings.update(self.local_settings)
            self.logger.info('Loaded settings.local.')
        except Exception as e:
            self.logger.warning('Cannot load settings.local: {}'.format(e))


    def apply_settings(self, new_settings):
        self.logger.debug('Appling settings.')


    def get_logger(self):
        logger = logging.getLogger(__name__)
        logger.propagate = 0
        logger.setLevel(level=logging.DEBUG if self.settings.get(
            'Debug') else logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(level=logging.DEBUG if self.settings.get(
            'Debug') else logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger


    def stop(self, exit=True):
        self.logger.info('Stopping...')
        self.save_settings()
        sys.stdout.flush()
        sys.stderr.flush()
        self._disconnect_sub_socket()
        self._disconnect_pub_socket()
        if exit:
            sys.exit(0)


    def spawn(self, func, *args, **kwargs):
        try:
            self.greenlets.append(gevent.spawn(func, *args, **kwargs))
        except Exception as e:
            self.logger.exception(repr(e))

    def spawn_later(self, delay, func, *args, **kwargs):
        try:
            self.greenlets.append(gevent.spawn_later(delay, func, *args, **kwargs))
        except Exception as e:
            self.logger.exception(repr(e))



    def run(self):
        self.logger.info('Started actor with uid {}.'.format(self.uid))
        gevent.joinall(self.greenlets)


    # Periodic function to check that connection is alive.
    def check_idle(self):
        idle_timeout = self.settings.get('IdleTimeout')
        if not idle_timeout:
            self.logger.info('Idle timeout watchdog disabled.')
            return
        self.logger.info('Idle timeout watchdog started.')
        while True:
            # Check when we last a message.
            now = time.time()
            if now - self.last_msg_time > idle_timeout:
                self.last_msg_time_sum += idle_timeout
                self.logger.warning(
                    'Idle timeout! No messages for last {} seconds.'.format(
                                                        self.last_msg_time_sum))
            gevent.sleep(idle_timeout)


    def subscribe(self, s):
        # Add additional subscriptions here.
        self.logger.debug('Subscribed for {}.'.format(s))
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b'|{}|'.format(s))


    def receive(self):
        # Actor sibscription receive loop
        self.logger.debug('Receiver has been started.')
        while True:
            try:
                header, msg = self.sub_socket.recv_multipart()
            except zmq.ZMQError as e:
                # This can be error due to ping() closing SUB socket.
                if self.last_pub_sub_reconnect - time.time() > 1:
                    self._disconnect_sub_socket()
                    self._connect_sub_socket()
                    self.logger.warning('SUB socket error: {}'.format(e))
                continue

            # Update counters
            self.receive_message_count +1
            self.last_msg_time = time.time()
            self.last_msg_time_sum = 0
            msg = json.loads(msg)
            msg.update({'Received': time.time()})

            # Check if it is a reply
            reply_to_id = msg.get('ReplyToId')
            if reply_to_id:
                # Yes, find who is waiting for it.
                if self.ask_pool.get(reply_to_id):
                    self.ask_pool[reply_to_id][
                        'result'] = msg
                    self.ask_pool[reply_to_id]['event'].set()
                else:
                    self.logger.error('Got an unexpected reply: {}'.format(
                        json.dumps(msg, indent=4)
                    ))
                continue
            # It's not a reply, so find for message handler
            else:
                if self.settings.get('Trace'):
                    self.logger.debug('Received: {}'.format(
                        json.dumps(msg, indent=4)
                    ))
                # Yes, a bit of magic here for easier use IMHO.
                if hasattr(self, 'on_{}'.format(msg.get('Message'))):
                    gevent.spawn(
                        getattr(
                            self, 'on_{}'.format(msg.get('Message'))), msg)
                else:
                    self.logger.debug('Don\'t know how to handle message: {}'.format(
                        json.dumps(msg, indent=4)))
                    continue



    def _remove_msg_headers(self, msg):
        res = msg.copy()
        for key in msg:
            if key in ['Id', 'ReplyToId', 'To', 'Received', 'From', 'Message',
                       'SendTime', 'Sequence']:
                res.pop(key)
        return res



    def tell(self, msg):
        # This is used to send a message to the bus.
        self.sent_message_count += 1
        msg.update({
            'Id': uuid.uuid4().hex,
            'SendTime': time.time(),
            'From': self.uid,
            'Sequence': self.sent_message_count,
        })
        if self.settings.get('Trace'):
            self.logger.debug('Telling: {}'.format(json.dumps(
                msg, indent=4
            )))
        self.pub_socket.send_json(msg)
        return msg


    def ask(self, msg, attempts=2, timeout=5):
        # This is used to send a message to the bus and wait for reply
        self.sent_message_count += 1
        msg_id = uuid.uuid4().hex
        msg.update({
            'Id': msg_id,
            'SendTime': time.time(),
            'From': self.uid,
            'ReplyTo': [self.uid]
        })
        if self.settings.get('Trace'):
            self.logger.debug('Asking: {}'.format(json.dumps(
                msg, indent=4
            )))
        self.ask_pool[msg_id] = {}
        self.ask_pool[msg_id] = {
            'event': Event(),
            'result': {},
        }
        self.pub_socket.send_json(msg)
        if self.ask_pool[msg_id]['event'].wait(timeout=timeout):
            # We got a reply
            result = self.ask_pool[msg_id]['result']
            del self.ask_pool[msg_id]
            if self.settings.get('Trace'):
                self.logger.debug('Reply received: {}'.format(
                    json.dumps(result, indent=4)
                ))
            return result
        else:
            # No reply was received
            self.logger.debug('No reply was received for {}'.format(
                json.dumps(msg, indent=4)
            ))
            return {}



    def heartbeat(self):
        gevent.sleep(1) # Delay before 1-st ping, also allows
        # HeartbeatInterval to be received from settings.
        heartbeat_interval = self.settings.get('HeartbeatInterval')
        if not heartbeat_interval:
            self.logger.info('Heartbeat disabled.')
            return
        else:
            self.logger.info('Starting heartbeat every {} seconds.'.format(heartbeat_interval))
        gevent.sleep(2) # Give time for subscriber to setup

        def reconnect_pub_sub():
            self._disconnect_pub_socket()
            self._disconnect_sub_socket()
            self._connect_sub_socket()
            self._connect_pub_socket()

        while True:
            ret = self.ask({
                'Message': 'Ping',
                'To': self.uid,
            }, timeout=self.settings.get('HeartbeatTimeout'))
            if not ret:
                self.last_pub_sub_reconnect = time.time()
                reconnect_pub_sub()
                self.logger.warning('PUB / SUB sockets reconnected.')
                gevent.sleep(1)

            gevent.sleep(heartbeat_interval)


    @check_reply
    def on_Ping(self, msg):
        From = msg.get('From') if msg.get('From') != self.uid else 'myself'
        self.logger.debug('Ping received from {}.'.format(From))
        if not msg.get('ReplyTo'):
            # Send Pong message only for tellers not askers.
            new_msg = {
                'Message': 'Pong',
                'To': msg.get('From'),
                'Version': self.version,
            }
            self.tell(new_msg)
        return {
            'Version': self.version,
        }


    def on_Pong(self, msg):
        From = msg.get('From') if msg.get('From') != self.uid else 'myself'
        self.logger.debug('Pong received from {}.'.format(From))



    @check_reply
    def on_UpdateSettings(self, msg):
        s = self._remove_msg_headers(msg)
        self.settings.update(s)
        self.apply_settings(s)
        self.save_settings()
        if self.settings.get('Trace'):
            self.logger.debug('[UpdateSettings] Received: {}'.format(
                json.dumps(s, indent=4)))
        else:
            self.logger.info('Settings updated.')


    def on_KeepAlive(self, msg):
        self.logger.debug('KeepAlive received.')


    # TODO: Ideas
    def on_Subscribe(self, msg):
        # TODO: Remote subscrube / unsubscribe
        pass


    def on_Start(self, msg):
        # Start /stop remotely local method
        pass
