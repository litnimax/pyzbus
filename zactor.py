import gevent
from gevent.monkey import patch_all; patch_all()
from gevent.queue import Queue
from gevent.event import Event
import json
import logging
import setproctitle
import signal
import time
import uuid
import zmq.green as zmq

logger = logging.getLogger(__name__)


class ZActor(object):
    uid = None
    greenlets = []

    receive_message_count = 0
    sent_message_count = 0
    last_msg_time = time.time()
    last_msg_time_sum = 0

    pub_socket = sub_socket = req_socket = None

    def __init__(self, uid=None,
                sub_addr='tcp://127.0.0.1:8881',
                pub_addr='tcp://127.0.0.1:8882',
                req_addr='tcp://127.0.0.1:8883',
                trace=False,
                ping_interval=0, idle_timeout=180):
        if uid:
            self.uid = uid
        else:
            self.uid = uuid.getnode()
        self.ping_interval = ping_interval
        self.idle_timeout = idle_timeout
        self.trace = trace

        self.context = zmq.Context()

        self.req_socket = self.context.socket(zmq.REQ)
        self.req_socket.connect(req_addr)

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect(pub_addr)

        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(sub_addr)

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


    def spawn(self, func):
        try:
            self.greenlets.append(gevent.spawn(func))
        except Exception as e:
            logger.exception(repr(e))

    def run(self):
        logger.info('Started actor with uid {}.'.format(self.uid))
        gevent.joinall(self.greenlets)


    # Periodic function to check that connection is alive.
    def check_idle(self):
        if not self.idle_timeout:
            return
        while True:
            # Check when we last a message.
            now = time.time()
            if now - self.last_msg_time > self.idle_timeout:
                self.last_msg_time_sum += self.idle_timeout
                logger.warning(
                    'Idle timeout! No messages for last {} seconds.'.format(
                                                        self.last_msg_time_sum))
            gevent.sleep(self.idle_timeout)


    def subscribe(self, s):
        # Add additional subscriptions here.
        logger.debug('Subscribed for {}.'.format(s))
        self.sub_socket.subscribe(b'{}'.format(s))


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
            if self.trace:
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


    def check_reply(self, msg, new_msg):
        if msg.get('ReplyRequest'):
            new_msg['ReplyToId'] = msg['Id']
            return True


    def tell(self, msg):
        # This is used to send a message to the bus.
        self.sent_message_count += 1
        msg.update({
            'Id': uuid.uuid4().hex,
            'SendTime': time.time(),
            'From': self.uid,
            'Sequence': self.sent_message_count,
        })
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
        self.req_socket.send_json(msg)
        return self.req_socket.recv_json()


    def ping(self):
        if not self.ping_interval:
            logger.info('Ping disabled.')
            return
        else:
            logger.info('Starting ping every {} seconds.'.format(self.ping_interval))
        gevent.sleep(2) # Give time for subscriber to setup
        while True:
            reply = self.ask({
                'Message': 'Ping',
                'To': self.uid,
            })
            if not reply:
                logger.warning('Ping reply is not received.')
            gevent.sleep(self.ping_interval)


    def watch(self, target):
        # TODO: heartbeat targer
        pass


    def on_Ping(self, msg):
        logger.debug('Ping received from {}, sending Pong'.format(msg.get('From')))
        new_msg = {
            'Message': 'Pong',
            'To': msg.get('From'),
        }
        self.check_reply(msg, new_msg)
        self.tell(new_msg)


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
