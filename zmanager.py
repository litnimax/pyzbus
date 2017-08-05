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


logger = logging.getLogger(__name__)


class ZManager(object):

    settings = {
        'PubAddr': 'tcp://127.0.0.1:8881', # Sending messages to agents
        'SubAddr': 'tcp://127.0.0.1:8882', # Collecting agent messages
        'MessageExpireTime': 5, # Discard all messages older then 10 seconds
        'Trace': False,
        'Debug': False,
        'KeepAlive': 170,
        'LocalSettingsFile': None,
    }

    pub_socket = sub_socket = None
    greenlets = []

    def __init__(self, settings={}):
        self.settings.update(settings)
        self.load_settings()
        logger.setLevel(
            logging.DEBUG if self.settings.get('Debug') else logging.INFO)
        self.context = zmq.Context()
        # Create publish socket
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.pub_socket.bind(self.settings.get('PubAddr'))
        # Subscribe socket for accepting messages
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.sub_socket.subscribe(b'')
        self.sub_socket.bind(self.settings.get('SubAddr'))
        # Init greenlets
        self.greenlets.append(spawn(self.do_KeepAlive))
        self.greenlets.append(spawn(self.sub_receive))


    def run(self):
        logger.info('ZManager has been started.')
        joinall(self.greenlets)


    def sub_receive(self):
        # Create a socket to collect incoming agent messages
        logger.debug('Starting receiver.')
        while True:
            try:
                msg = self.sub_socket.recv_json()
                if self.settings.get('Trace'):
                    if msg.get('ReplyToId'):
                        logger.debug('[REPLIED] {}'.format(
                            json.dumps(msg, indent=4)))
                    else:
                        logger.debug('[RECEIVED] {}'.format(
                            json.dumps(msg, indent=4)))
                # Check expiration
                time_diff = abs(time.time() - msg.get('SendTime', 0))
                logger.debug('{} from {} time difference: {}'.format(
                    msg.get('Message'), msg.get('From'), time_diff))
                exp_time = float(self.settings.get('MessageExpireTime'))
                threashold = 1
                if time_diff >= exp_time and time_diff <= exp_time + threashold:
                    # Give a WARNING on 1 second before discard
                    logger.warning(
                        'Nearly expired ({} seconds) message {}.'.format(
                            time_diff, json.dumps(msg, indent=4)))
                elif time_diff > exp_time + threashold:
                    logger.error(
                        'Discarding expired ({} seconds) message {}.'.format(
                            time_diff, json.dumps(msg, indent=4)))
                    # Next message please...
                    continue

                self.pub_socket.send_multipart(['|{}|'.format(msg.get('To')),
                                          json.dumps(msg)])

            except Exception as e:
                logger.exception(e)


    def do_KeepAlive(self):
        # Periodic keep alive message to all connected actors
        if not self.settings.get('KeepAlive'):
            logger.info('KeepAlive disabled.')
            return
        logger.info('Starting publishing KeepAlive messages with rate {}.'.format(
            self.settings.get('KeepAlive')))
        while True:
            logger.debug('Publishing KeepAlive.')
            msg = {
                'Message': 'KeepAlive',
                'SendTime': time.time(),
                'SendTimeHuman': datetime.strftime(datetime.now(),
                                                   '%Y-%m-%d %H:%M:%S')
            }
            self.pub_socket.send_multipart(['|*|', json.dumps(msg)])
            gevent.sleep(self.settings.get('KeepAlive'))


    def load_settings(self):
        if not self.settings.get('LocalSettingsFile'):
            logger.info('LocalSettingsFile not set, local settings not loaded.')
            return
        try:
            self.local_settings = json.loads(
                open(self.settings.get('LocalSettingsFile')).read())
            self.settings.update(self.local_settings)
            logger.debug('Loaded settings.local.')
        except Exception as e:
            logger.warning('Cannot load settings.local: {}'.format(e))
