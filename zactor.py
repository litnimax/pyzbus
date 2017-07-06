import gevent
from gevent.monkey import patch_all; patch_all()
from gevent.queue import Queue
from gevent.event import Event
import json
import logging
import setproctitle
import signal
import sys
import time
import uuid
import zmq.green as zmq


# Decorators
def check_reply(func):
    def wrapper(agent, msg, *args, **kwargs):
        res = func(agent, msg, *args, **kwargs)
        if msg.get('ReplyRequest'):
            if not res:
                res = {}
            reply = {
                # No 'To' header here as reply is returned by ask()
                'Message': '{}Reply'.format(msg.get('Message')),
                'ReplyToId': msg['Id']
            }
            reply.update(res) # Update reply with results from func.
            agent.tell(reply)
        return res
    return wrapper



class ZActor(object):
    uid = None
    greenlets = []

    receive_message_count = 0
    sent_message_count = 0
    last_msg_time = time.time()
    last_msg_time_sum = 0
    pong_event = Event()
    last_pub_sub_reconnect = None
    pub_socket = sub_socket = req_socket = None


    settings = {
        'UID': False,
        'SubAddr': 'tcp://127.0.0.1:8881',
        'PubAddr': 'tcp://127.0.0.1:8882',
        'ReqAddr': 'tcp://127.0.0.1:8883',
        'PingInterval': 0,
        'IdleTimeout': 200,
        'Trace': True,
        'Debug': True,
    }

    def __init__(self, settings={}):
        self.logger = self.get_logger()
        # Update startup settings
        self.load_settings(settings)
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
        self._connect_sub_socket()
        self._connect_pub_socket()
        self._connect_req_socket()

        # gevent.spawn Greenlets
        self.greenlets.append(gevent.spawn(self.check_idle))
        self.greenlets.append(gevent.spawn(self.receive))
        self.greenlets.append(gevent.spawn(self.ping))
        # Install signal handler
        gevent.signal(signal.SIGINT, self.stop)
        gevent.signal(signal.SIGTERM, self.stop)


    def _connect_req_socket(self):
        self.req_socket = self.context.socket(zmq.REQ)
        self.req_socket.connect(self.settings.get('ReqAddr'))
        self.logger.debug('Connected REQ socket.')

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

    def _disconnect_req_socket(self):
        self.req_socket.setsockopt(zmq.LINGER, 0)
        self.req_socket.close()
        self.logger.debug('Disconnected REQ socket.')


    def save_settings(self):
        try:
            with open('settings.cache', 'w') as file:
                file.write('{}\n'.format(
                    json.dumps(self.settings, indent=4)
                ))
            self.logger.debug('Saved settings.cache')
        except Exception as e:
            self.logger.error('Cannot save settings.cache: {}'.format(e))


    def load_settings(self, settings):
        self.settings.update(settings)
        try:
            settings_cache = json.loads(open('settings.cache').read())
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
        self.logger.info('Apply settings not implemented')


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


    def stop(self):
        self.logger.info('Stopping...')
        self._disconnect_req_socket()
        self._disconnect_sub_socket()
        self._disconnect_pub_socket()


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
            if self.settings.get('Trace'):
                self.logger.debug('Received: {}'.format(json.dumps(msg, indent=4)))

            # Yes, a bit of magic here for easier use IMHO.
            if hasattr(self, 'on_{}'.format(msg.get('Message'))):
                gevent.spawn(
                    getattr(
                        self, 'on_{}'.format(msg.get('Message'))), msg)
            else:
                self.logger.error('Don\'t know how to handle message: {}'.format(
                    json.dumps(msg, indent=4)))
                continue


    def handle_failure(self, msg):
        # TODO: When asked and got an error, send reply with error info.
        pass


    def _remove_msg_headers(self, msg):
        res = msg.copy()
        for key in msg:
            if key in ['Id', 'To', 'Received', 'From', 'Message', 'SendTime', 'Sequence']:
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
            'Timeout': timeout,
        })
        if self.settings.get('Trace'):
            self.logger.debug('Asking: {}'.format(json.dumps(
                msg, indent=4
            )))
        try:
            req_socket = self.context.socket(zmq.REQ)
            req_socket.connect(self.settings.get('ReqAddr'))
            req_socket.send_json(msg)
            poll = zmq.Poller()
            poll.register(req_socket, zmq.POLLIN)
            socks = dict(poll.poll(timeout*1000))
            if socks.get(req_socket) == zmq.POLLIN:
                res = req_socket.recv_json()
                return res
            else:
                self.logger.warning('No reply received for {}.'.format(json.dumps(msg,
                                                                    indent=4)))
                req_socket.setsockopt(zmq.LINGER, 0)
                poll.unregister(req_socket)
                req_socket.close()
                # Re-Ask second time
                if attempts > 1:
                    self.logger.debug('Asking again.')
                    attempts -= 1
                    self.ask(msg, attempts=attempts)
                else:
                    self.logger.debug('Forgetting.')
                    return {}
        except Exception as e:
            self.logger.error('[Ask] {}'.format(e))
            raise


    def ping(self):
        gevent.sleep(1) # Delay before 1-st ping, also allows
        # PingInterval to be received from settings.
        ping_interval = self.settings.get('PingInterval')
        if not ping_interval:
            self.logger.info('Ping disabled.')
            return
        else:
            self.logger.info('Starting ping every {} seconds.'.format(ping_interval))
        gevent.sleep(2) # Give time for subscriber to setup


        def reconnect_pub_sub():
            self.last_pub_sub_reconnect = time.time()
            self._disconnect_pub_socket()
            self._disconnect_sub_socket()
            self._connect_sub_socket()
            self._connect_pub_socket()


        while True:
            # Check REQ socket and SUB sockets at once.
            reply = self.ask({
                'Message': 'Ping',
                'To': self.uid,
            })
            if not reply:
                self.logger.warning('Ping reply is not received.')
            # Now check PUB socket
            ret = self.tell({
                'Message': 'Ping',
                'To': self.uid,
            })
            if not self.pong_event.wait(timeout=5):
                # Timeout, no ping at all.
                reconnect_pub_sub()
                self.logger.warning('PUB / SUB sockets reconnected.')
                gevent.sleep(1)

            else:
                # We got an on_Pong event
                self.pong_event.clear()
                if self.last_ping_id != ret.get('Id'):
                    self.logger.warning('Last ping Id {} != {}!'.format(
                        self.last_ping_id, ret.get('Id')
                    ))
                    reconnect_pub_sub()
                    self.logger.warning('PUB / SUB sockets reconnected.')
                    gevent.sleep(1)

            gevent.sleep(ping_interval)


    def watch(self, target):
        # TODO: heartbeat targer
        pass


    @check_reply
    def on_Ping(self, msg):
        From = msg.get('From') if msg.get('From') != self.uid else 'myself'
        self.logger.debug('Ping received from {}.'.format(From))
        if not msg.get('ReplyRequest'):
            # Send Pong message only for tellers not askers.
            new_msg = {
                'Message': 'Pong',
                'To': msg.get('From'),
                'PingId': msg.get('Id'),
            }
            self.tell(new_msg)


    def on_Pong(self, msg):
        From = msg.get('From') if msg.get('From') != self.uid else 'myself'
        self.logger.debug('Pong received from {}.'.format(From))
        self.last_ping_id = msg.get('PingId')
        self.pong_event.set()



    @check_reply
    def on_UpdateSettings(self, msg):
        s = self._remove_msg_headers(msg)
        if self.settings.get('Trace'):
            self.logger.debug('[UpdateSettings] Received: {}'.format(
                json.dumps(s, indent=4)))
        else:
            self.logger.info('Settings updated.')
        self.settings.update(s)
        self.apply_settings(s)
        self.save_settings()


    def on_KeepAlive(self, msg):
        self.logger.debug('KeepAlive received.')


    # TODO: Ideas
    def on_Subscribe(self, msg):
        # TODO: Remote subscrube / unsubscribe
        pass


    def on_Start(self, msg):
        # Start /stop remotely local method
        pass
