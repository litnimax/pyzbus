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

logger = logging.getLogger(__name__)

# Decorators
def check_reply(func):
    def wrapper(agent, msg):
        if msg.get('ReplyRequest'):
            res = func(agent, msg)
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

    pub_socket = sub_socket = req_socket = None

    settings = {
        'SubAddr': 'tcp://127.0.0.1:8881',
        'PubAddr': 'tcp://127.0.0.1:8882',
        'ReqAddr': 'tcp://127.0.0.1:8883',
        'PingInterval': 0,
        'IdleTimeout': 180,
        'Trace': True,
        'Debug': True,
    }

    def __init__(self, uid=None, settings={}):
        # Adjust debug level
        logger.setLevel(
            level=logging.DEBUG if self.settings.get('Debug') else logging.INFO)
        if uid:
            self.uid = uid
        else:
            self.uid = str(uuid.getnode())
        logging.info('Agent UID: {}'.format(self.uid))
        # Update startup settings
        self.settings.update(settings)

        self.context = zmq.Context()

        self.req_socket = self.context.socket(zmq.REQ)
        self.req_socket.connect(self.settings.get('ReqAddr'))

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect(self.settings.get('PubAddr'))

        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(self.settings.get('SubAddr'))

        self.sub_socket.setsockopt(zmq.IDENTITY, self.uid)
        self.pub_socket.setsockopt(zmq.IDENTITY, self.uid)
        # Subscribe to messages for actor and also broadcasts
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b'|{}|'.format(self.uid))
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b'|*|')
        # gevent.spawn Greenlets
        self.greenlets.append(gevent.spawn(self.check_idle))
        self.greenlets.append(gevent.spawn(self.receive))
        self.greenlets.append(gevent.spawn(self.ping))
        # Install signal handler
        gevent.signal(signal.SIGINT, self.stop)
        gevent.signal(signal.SIGTERM, self.stop)


    def stop(self):
        logging.debug('Stopping.')
        self.sub_socket.close()
        self.pub_socket.close()
        sys.exit(0)


    def spawn(self, func, *args, **kwargs):
        try:
            self.greenlets.append(gevent.spawn(func, *args, **kwargs))
        except Exception as e:
            logger.exception(repr(e))

    def run(self):
        logger.info('Started actor with uid {}.'.format(self.uid))
        gevent.joinall(self.greenlets)


    # Periodic function to check that connection is alive.
    def check_idle(self):
        idle_timeout = self.settings.get('IdleTimeout')
        if not idle_timeout:
            logger.info('Idle timeout watchdog disabled.')
            return
        logger.info('Idle timeout watchdog started.')
        while True:
            # Check when we last a message.
            now = time.time()
            if now - self.last_msg_time > idle_timeout:
                self.last_msg_time_sum += idle_timeout
                logger.warning(
                    'Idle timeout! No messages for last {} seconds.'.format(
                                                        self.last_msg_time_sum))
            gevent.sleep(idle_timeout)


    def subscribe(self, s):
        # Add additional subscriptions here.
        logger.debug('Subscribed for {}.'.format(s))
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b'|{}|'.format(s))


    def receive(self):
        # Actor sibscription receive loop
        logger.debug('Receiver has been started.')
        while True:
            header, msg = self.sub_socket.recv_multipart()
            # Update counters
            self.receive_message_count +1
            self.last_msg_time = time.time()
            self.last_msg_time_sum = 0
            msg = json.loads(msg)
            msg.update({'Received': time.time()})
            if self.settings.get('Trace'):
                logger.debug('Received: {}'.format(json.dumps(msg, indent=4)))

            # Yes, a bit of magic here for easier use IMHO.
            if hasattr(self, 'on_{}'.format(msg.get('Message'))):
                gevent.spawn(
                    getattr(
                        self, 'on_{}'.format(msg.get('Message'))), msg)
            else:
                logger.error('Don\'t know how to handle message: {}'.format(
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
            logger.debug('Telling: {}'.format(json.dumps(
                msg, indent=4
            )))
        self.pub_socket.send_json(msg)


    def ask(self, msg):
        # This is used to send a message to the bus and wait for reply
        self.sent_message_count += 1
        msg_id = uuid.uuid4().hex
        msg.update({
            'Id': msg_id,
            'SendTime': time.time(),
            'From': self.uid,
        })
        if self.settings.get('Trace'):
            logger.debug('Asking: {}'.format(json.dumps(
                msg, indent=4
            )))
        try:
            self.req_socket.send_json(msg)
            res = self.req_socket.recv_json()
            if not res:
                logging.warning('No reply received for {}.'.format(json.dumps(msg,
                                                                    indent=4)))
            return res

        except zmq.ZMQError as e:
            logger.error('Ask ZMQ error: {}'.format(e))
            print 111, 'Operation cannot be accomplished in current state' in repr(e)


    def ping(self):
        ping_interval = self.settings.get('PingInterval')
        if not ping_interval:
            logger.info('Ping disabled.')
            return
        else:
            logger.info('Starting ping every {} seconds.'.format(ping_interval))
        gevent.sleep(2) # Give time for subscriber to setup
        while True:
            reply = self.ask({
                'Message': 'Ping',
                'To': self.uid,
            })
            if not reply:
                logger.warning('Ping reply is not received.')
            gevent.sleep(ping_interval)


    def watch(self, target):
        # TODO: heartbeat targer
        pass


    @check_reply
    def on_Ping(self, msg):
        logger.debug('Ping received from {}, sending Pong'.format(msg.get('From')))
        new_msg = {
            'Message': 'Pong',
            'To': msg.get('From'),
        }
        self.tell(new_msg)


    def save_settings(self):
        logger.info('Settings saved in memory :-)')


    def apply_settings(self):
        logger.info('Apply settings not implemented')


    def on_UpdateSettings(self, msg):
        s = self._remove_msg_headers(msg)
        if self.settings.get('Trace'):
            logger.debug('[UpdateSettings] Received: {}'.format(
                json.dumps(s, indent=4)))
        else:
            logger.info('Settings updated.')
        self.settings.update(s)
        self.apply_settings()
        self.save_settings()


    def on_KeepAlive(self, msg):
        logger.debug('KeepAlive received.')


    def on_Pong(self, msg):
        logger.debug('Pong received.')


    # TODO: Ideas
    def on_Subscribe(self, msg):
        # TODO: Remote subscrube / unsubscribe
        pass


    def on_Start(self, msg):
        # Start /stop remotely local method
        pass
