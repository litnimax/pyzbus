#!/usr/bin/env python2.7

import gevent
from gevent.monkey import patch_all; patch_all()
from gevent import wsgi, spawn, joinall
from gevent.event import Event
import json
import logging
import os
import setproctitle
import sys
import time
import zmq.green as zmq
#from zmq.auth import Authenticator

PUB_ADDR = 'tcp://127.0.0.1:8881' # Sending messages to agents
SUB_ADDR = 'tcp://127.0.0.1:8882' # Collecting agent messages
REP_ADDR = 'tcp://127.0.0.1:8883' # Request / Reply (Ask)

MESSAGE_EXPIRE_TIME = 5 # Discard all messages older then 10 seconds

logger = logging.getLogger(__name__)


"""
auth = Authenticator(context=context)
auth.allow('192.168.1.1')
auth.configure_plain('*', os.path.join(os.path.dirname(__file__), 'passwd'))
auth.start()
"""

class ZManager(object):

    pub_socket = sub_socket = rep_socket = None

    greenlets = []

    rep_wait_pool = {}

    def __init__(self, keepalive=120, sub_addr=SUB_ADDR, pub_addr=PUB_ADDR,
                 rep_addr=REP_ADDR, debug=False, trace=False):
        self.keepalive = keepalive
        self.trace = trace
        if debug:
            self.debug = True
            logger.setLevel(logging.DEBUG)
        self.context = zmq.Context()
        # Create publish socket
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind(pub_addr)
        # Subscribe socket for accepting messages
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.subscribe(b'')
        self.sub_socket.bind(sub_addr)
        # Create REP sockets
        self.rep_socket = self.context.socket(zmq.REP)
        self.rep_socket.bind(rep_addr)
        # Init greenlets
        self.greenlets.append(spawn(self.do_keepalive))
        self.greenlets.append(spawn(self.sub_receive))
        self.greenlets.append(spawn(self.rep_receive))


    def run(self):
        joinall(self.greenlets)


    def sub_receive(self):
        # Create a socket to collect incoming agent messages
        logger.debug('Starting receiver.')
        while True:
            try:
                msg = self.sub_socket.recv_json()
                if self.trace:
                    if msg.get('ReplyToId'):
                        logger.debug('[REPLIED] {}'.format(
                            json.dumps(msg, indent=4)))
                    else:
                        logger.debug('[RECEIVED] {}'.format(
                            json.dumps(msg, indent=4)))
                # Check expiration
                time_diff = time.time() - msg.get('SendTime', 0)
                if time_diff > MESSAGE_EXPIRE_TIME:
                    logger.info(
                        'Discarding expired ({} seconds) message {}.'.format(
                            time_diff, msg))
                    continue

                # Check if it a reply, if so pass the message to internal reply wait pool.
                reply_to_id = msg.get('ReplyToId')
                if reply_to_id:
                    if self.rep_wait_pool.get(reply_to_id):
                        # There is waiting message, put reply there
                        self.rep_wait_pool.get(reply_to_id)['reply'] = msg
                        self.rep_wait_pool.get(reply_to_id)['event'].set()

                    else:
                        # Reply to a message that is not expected.
                        logger.error('Reply to non-existent message: {}'.format(msg))

                self.pub_socket.send_multipart(['|{}|'.format(msg.get('To', 'ToNotSpecified')),
                                          json.dumps(msg)])

            except Exception as e:
                logger.exception(e)


    def rep_receive(self, timeout=5):
        logger.debug('Starting replier.')
        while True:
            try:
                msg = self.rep_socket.recv_json()
                msg_id = msg.get('Id')
                if self.trace:
                    logger.debug('[REQUESTED] {}'.format(json.dumps(msg, indent=4)))

                # Replicate to subscriptions
                msg['ReplyRequest'] = True
                self.pub_socket.send_multipart(['|{}|'.format(msg.get('To')),
                                               json.dumps(msg)])
                self.rep_wait_pool[msg_id] = {
                    'event':Event(),
                    'reply': {}, # Default message when no reply is got.
                }
                self.rep_wait_pool[msg_id]['event'].wait(timeout)
                reply = self.rep_wait_pool[msg_id]['reply']
                if not reply:
                    logger.warning('No reply received for message {}'.format(
                        json.dumps(msg, indent=4)))
                    del self.rep_wait_pool[msg_id]
                    self.rep_socket.send_json({})
                else:
                    ret = reply and reply.copy()
                    del self.rep_wait_pool[msg_id]
                    self.rep_socket.send_json(ret)

            except Exception as e:
                logger.exception(e)
                sys.exit(0)



    def do_keepalive(self):
        # Periodic keep alive message to all connected actors
        if not self.keepalive:
            logger.info('KeepAlive disabled.')
            return
        logger.info('Starting publish keepalive with rate {}.'.format(self.keepalive))
        while True:
            logger.debug('Publishing keepalive.')
            msg = {'Message': 'KeepAlive'}
            self.pub_socket.send_multipart(['|*|', json.dumps(msg)])
            gevent.sleep(self.keepalive)
