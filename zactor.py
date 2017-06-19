import gevent
from gevent.monkey import patch_all; patch_all()
from gevent.queue import Queue
from gevent.event import Event
import json
import logging
import setproctitle
import time
import uuid
import zmq.green as zmq

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

context = zmq.Context()


class ZActor(object):
    uid = None
    greenlets = []

    receive_message_count = 0
    sent_message_count = 0
    last_msg_time = time.time()
    last_msg_time_sum = 0

    reply_wait_pool = {}

    pub_socket = context.socket(zmq.PUB)
    sub_socket = context.socket(zmq.SUB)

    def __init__(self, uid=None, sub_addr='tcp://127.0.0.1:8881',
                pub_addr='tcp://127.0.0.1:8882',
                ping_interval=0, idle_timeout=30):
        if uid:
            self.uid = uid
        else:
            self.uid = uuid.getnode()
        self.ping_interval = ping_interval
        self.idle_timeout = idle_timeout

        # Connect sockets
        self.pub_socket.connect(pub_addr)
        self.sub_socket.connect(sub_addr)
        # Subscribe to messages for actor and also broadcasts
        self.sub_socket.subscribe('|{}|'.format(self.uid))
        self.sub_socket.subscribe('|*|'.format(self.uid))

        # gevent.spawn Greenlets
        self.greenlets.append(gevent.spawn(self.check_idle))
        self.greenlets.append(gevent.spawn(self.receive))
        self.greenlets.append(gevent.spawn(self.ping))


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
            logger.debug('Received: {}'.format(json.dumps(msg, indent=4)))

            # Check if it a reply, if so pass the message to internal reply wait pool.
            if msg.get('ReplyId'):
                if self.reply_wait_pool.get(msg.get('ReplyId')):
                    # There is waiting message, put reply there
                    self.reply_wait_pool.get(msg.get('ReplyId'))['reply'] = msg
                    self.reply_wait_pool.get(msg.get('ReplyId'))['event'].set()
                    continue
                else:
                    # Reply to a message that is not expected.
                    logger.error('Reply to non-existent message: {}'.format(msg))
                    continue
            # Yes, a bit of magic here for easier use IMHO.
            if hasattr(self, 'on_{}'.format(msg.get('Message'))):
                gevent.spawn(
                    getattr(
                        self, 'on_{}'.format(msg.get('Message')))(msg))
            else:
                logger.error('Don\'t know how to handle message Id {}'.format(
                                                                msg.get('Id')))
                continue


    def handle_failure(self, msg):
        # TODO: When asked and got an error, send reply with error info.
        pass


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


    def ask(self, msg, timeout=5):
        # This is used to send a message to the bus and wait for reply
        self.sent_message_count += 1
        msg_id = uuid.uuid4().hex
        msg.update({
            'Id': msg_id,
            'SendTime': time.time(),
            'From': self.uid,
            # WTF is that needed? 'Sequence': self.sent_message_count,
        })
        self.pub_socket.send_json(msg)
        self.reply_wait_pool[msg_id] = {
            'event':Event(),
            'reply': None}
        self.reply_wait_pool[msg_id]['event'].wait(timeout)
        reply = self.reply_wait_pool[msg_id]['reply']
        if not reply:
            logger.warning('No reply received for message {}'.format(
                json.dumps(msg, indent=4)))

        ret = reply and reply.copy()
        del self.reply_wait_pool[msg_id]
        return ret


    def ping(self):
        if not self.ping_interval:
            logger.info('Ping disabled.')
            return
        else:
            logger.info('Starting ping every {} seconds.'.format(self.ping_interval))
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
        self.tell({
            'Message': 'Pong',
            'To': msg.get('From'),
            'ReplyId': msg.get('Id'),
        })


    def on_KeepAlive(self, msg):
        logger.debug('KeepAlive received.')


    # TODO: Ideas
    def on_Subscribe(self, msg):
        # TODO: Remote subscrube / unsubscribe
        pass


    def on_Start(self, msg):
        # Start /stop remotely local method
        pass
