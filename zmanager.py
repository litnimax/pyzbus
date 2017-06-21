#!/usr/bin/env python2.7

import gevent
from gevent.monkey import patch_all; patch_all()
from gevent import wsgi, spawn, joinall
import json
import logging
import setproctitle
import time
import zmq.green as zmq

PUB_PORT = 8881 # Sending messages to agents
SUB_PORT = 8882 # Collecting agent messages

MESSAGE_EXPIRE_TIME = 2 # Discard all messages older then 10 seconds

logger = logging.getLogger(__name__)

context = zmq.Context()

class ZManager(object):

    pub_socket = context.socket(zmq.PUB)
    sub_socket = context.socket(zmq.SUB)

    greenlets = []

    def __init__(self, keepalive=120, sub_addr='tcp://127.0.0.1:8882',
                 pub_addr='tcp://127.0.0.1:8881', debug=False):
        self.keepalive = keepalive
        if debug:
            self.debug = True
            logger.setLevel(logging.DEBUG)
        # Create publish socket
        self.pub_socket = context.socket(zmq.PUB)
        self.pub_socket.bind(pub_addr)
        # Subscribe socket for accepting messages
        self.sub_socket.subscribe(b'')
        self.sub_socket.bind(sub_addr)
        # Init greenlets
        self.greenlets.append(spawn(self.do_keepalive))
        self.greenlets.append(spawn(self.receive))


    def run(self):
        joinall(self.greenlets)


    def receive(self):
        # Create a socket to collect incoming agent messages
        logger.info('Starting receiver.')
        while True:
            try:
                msg = self.sub_socket.recv_json()
                logger.debug('{}'.format(json.dumps(msg, indent=4)))
                # Check expiration
                time_diff = time.time() - msg.get('SendTime', 0)
                if time_diff > MESSAGE_EXPIRE_TIME:
                    logger.info(
                        'Discarding expired ({} seconds) message {}.'.format(
                            time_diff, msg))
                    continue
                #logger.debug('Sending to Pub: {}'.format(msg))
                self.pub_socket.send_multipart(['|{}|'.format(msg.get('To', 'ToNotSpecified')),
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
            msg = {'Message': 'KeepAlive'}
            self.pub_socket.send_multipart(['|*|', json.dumps(msg)])
            gevent.sleep(self.keepalive)
