#!/usr/bin/env python

"""
@package ion.agents.platform.oms.oms_alarm_listener
@file    ion/agents/platform/oms/oms_alarm_listener.py
@author  Carlos Rueda
@brief   HTTP server to get RSN OMS alarm notifications
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from ion.agents.platform.platform_driver import AlarmDriverEvent

import time
from gevent.pywsgi import WSGIServer
import yaml



class OmsAlarmListener(object):
    """
    HTTP server to get RSN OMS alarm notifications and do corresponding
    notifications to driver/agent via callback.
    """

    def __init__(self, notify_driver_event):
        """
        Creates a listener.

        @param notify_driver_event callback to notify alarm events. Must be
                                    provided.
        """

        assert notify_driver_event, "notify_driver_event callback must be provided"
        self._notify_driver_event = notify_driver_event

        self._http_server = None
        self._url = None

        # _notifications: if not None, { alarm_type: [alarm_instance, ...], ...}
        self._notifications = None

    @property
    def url(self):
        """
        The URL that can be used to register a listener to the OMS.
        This is None if there is no HTTP server currently running.
        """
        return self._url

    def keep_notifications(self, keep=True, reset=True):
        """
        By default, received alarm notifications are not kept. Call this with
        True (the default) to keep them, or with False to not keep them.
        If they are currently kept and the reset param is True (the default),
        then the notifications dict is reinitialized.
        """
        if keep:
            if not self._notifications or reset:
                self._notifications = {}
        else:
            self._notifications = None

    @property
    def notifications(self):
        """
        The current dict of received notifications. This will be None if such
        notifications are not being kept.
        """
        return self._notifications

    def start_http_server(self, host='localhost', port=0):
        """
        Starts a HTTP server that handles the notification of received alarms.

        @param host by default 'localhost'
        @param port by default 0 to get one dynamically.
        """

        # reinitialize notifications if we are keeping them:
        if self._notifications:
            self._notifications.clear()

        self._http_server = WSGIServer((host, port), self.__application)
        log.info("starting http server for receiving alarm notifications...")
        self._http_server.start()
        self._url = "http://%s:%s" % self._http_server.address
        log.info("http server started: url=%r", self._url)

    def __application(self, environ, start_response):

        input = environ['wsgi.input']
        body = "\n".join(input.readlines())
#        log.trace('notification received payload=%s', body)
        alarm_instance = yaml.load(body)
        log.trace('notification received alarm_instance=%s', str(alarm_instance))
        if not 'url' in alarm_instance:
            log.warn("expecting 'url' entry in notification call")
            return
        if not 'ref_id' in alarm_instance:
            log.warn("expecting 'ref_id' entry in notification call")
            return

        url = alarm_instance['url']
        alarm_type = alarm_instance['ref_id']

        if self._url == url:
            self._alarm_received(alarm_type, alarm_instance)
        else:
            log.warn("got notification call with an unexpected url=%s (expected url=%s)",
                     url, self._url)

        # generic OK response  TODO determine appropriate variations
        status = '200 OK'
        headers = [('Content-Type', 'text/plain')]
        start_response(status, headers)
        return alarm_type

    def _alarm_received(self, alarm_type, alarm_instance):
        log.trace('received alarm_instance=%s', str(alarm_instance))

        if self._notifications:
            if alarm_type in self._notifications:
                self._notifications[alarm_type].append(alarm_instance)
            else:
                self._notifications[alarm_type] = [alarm_instance]

        log.debug('notifying alarm_instance=%s', str(alarm_instance))

        # note the ts for the event is the time of reception -- the
        # alarm_instance has its own timestamp
        ts = time.time()

        driver_event = AlarmDriverEvent(ts, alarm_type, alarm_instance)
        self._notify_driver_event(driver_event)

    def stop_http_server(self):
        """
        Stops the http server.
        @retval the dict of received notifications or None if they are not kept.
        """
        if self._http_server:
            log.info("HTTP SERVER: stopping http server: url=%r", self._url)
            self._http_server.stop()

        self._http_server = None
        self._url = None

        return self._notifications