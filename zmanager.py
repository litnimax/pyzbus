#!/usr/bin/env python2.7

import gevent
from gevent.monkey import patch_all; patch_all()
from gevent import wsgi, spawn, joinall
from gevent.event import Event
from datetime import datetime
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

MESSAGE_EXPIRE_TIME = 5 # Discard all messages older then 10 seconds

logger = logging.getLogger(__name__)


class ZManager(object):

    pub_socket = sub_socket = None
    greenlets = []

    def __init__(self, keepalive=170, sub_addr=SUB_ADDR, pub_addr=PUB_ADDR,
                 debug=False, trace=False):
        self.keepalive = keepalive
        self.trace = trace
        if debug:
            self.debug = True
            logger.setLevel(logging.DEBUG)
        self.context = zmq.Context()
        # Create publish socket
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.pub_socket.bind(pub_addr)
        # Subscribe socket for accepting messages
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.sub_socket.subscribe(b'')
        self.sub_socket.bind(sub_addr)
        # Init greenlets
        self.greenlets.append(spawn(self.do_keepalive))
        self.greenlets.append(spawn(self.sub_receive))


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
                time_diff = abs(time.time() - msg.get('SendTime', 0))
                logger.debug('Time difference: {}'.format(time_diff))
                if time_diff > MESSAGE_EXPIRE_TIME:
                    logger.warning(
                        'Discarding expired ({} seconds) message {}.'.format(
                            time_diff, json.dumps(msg, ident=4)))
                    continue

                self.pub_socket.send_multipart(['|{}|'.format(msg.get('To')),
                                          json.dumps(msg)])

            except Exception as e:
                logger.exception(e)


    def do_keepalive(self):
        # Periodic keep alive message to all connected actors
        if not self.keepalive:
            logger.info('KeepAlive disabled.')
            return
        logger.info('Starting publish keepalive with rate {}.'.format(self.keepalive))
        while True:
            logger.debug('Publishing keepalive.')
            msg = {
                'Message': 'KeepAlive',
                'SendTime': time.time(),
                'SendTimeHuman': datetime.strftime(datetime.now(),
                                                   '%Y-%m-%d %H:%M:%S')
            }
            self.pub_socket.send_multipart(['|*|', json.dumps(msg)])
            gevent.sleep(self.keepalive)
