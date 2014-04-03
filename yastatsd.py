# -*- coding: utf-8 -*-
# vim:sw=4:ts=4:sts=4:et:
#
# Copyright (c) 2014, Bryan Davis and contributors.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
Yet another StatsD server

Collects events submitted by client applications via UDP. Events are converted
into aggregate carbon/graphite metric statements on a periodic basis and
relayed to a carbon server.

Adapted from statsd python port by https://github.com/sivy/py-statsd
"""
from __future__ import division

import collections
import logging
import math
import re
import socket
import struct
import threading
import time

try:
    import cPickle as pickle
except ImportError:
    import pickle


CLEAN_RE = re.compile('[^\w\.-]')
METRIC_RE = re.compile('^(?P<name>[^:]+):(?P<value>[^|]+)\|(?P<opts>.+)$')


class Stats(object):

    def __init__(self):
        self.population = []
        self.samples = 0
        self.total = 0
        self.max = float('-inf')
        self.min = float('inf')
        self.sum_of_squares = 0
        self.sorted = True

    def add(self, value):
        """Record a new sample"""
        self.sorted = False
        self.population.append(value)
        self.samples += 1
        self.total += value
        if value > self.max:
            self.max = value
        if value < self.min:
            self.min = value
        self.sum_of_squares += value * value
        return self

    def _sort(self):
        """Sort sample population"""
        if not self.sorted:
            self.population.sort()
            self.sorted = True

    @property
    def stdev(self):
        """Compute the standard deviation of the input set.

        This std deviation caclulation does not require keeping all data
        points. xi below stands for the ith data point.

        Required info to calculate:
            - n: count of all x's
            - sum(xi): running total of sample values
            - sum(xi^2): running total of sample values squared
        numerator = sum(xi^2) - (sum(xi)^2) / n
        std_dev = square_root( numerator / (n-1) )

        Algorithm from www.jamonapi.com
        """
        n = self.samples
        if n == 0:
            return 0
        if n == 1:
            return 0
        sumOfX = self.total
        nMinus1 = 1 if n <= 1 else n - 1  # avoid 0 divides
        numerator = self.sum_of_squares - ((sumOfX * sumOfX) / n)
        return math.sqrt(numerator / nMinus1)

    def percentile(self, pcnt):
        """Find the Nth percentile from a sorted population.

        Computes using linear interpolation if exact match is not found in
        population.

        :param pcnt: Percentile to calculate
        :type pcnt: int
        :return: Calculated percentile value
        :rtype: numeric
        """
        self._sort()
        n = self.samples
        if n == 0:
            return 0

        k = pcnt * (n + 1) / 100
        i = min(max(int(math.floor(k)), 0), n - 1)
        j = min(int(math.ceil(k)), n - 1)
        try:
            if i == j:
                return self.population[i]

            else:
                # linear interpolation
                pi = self.population[i]
                pj = self.population[j]
                return pi + (k - i) * (pj - pi)
        except IndexError:
            return 0

    def below_percentile(self, pcnt):
        """Get the samples at and below the given percentile.

        Args:
            pcnt: percentile to calculate

        Returns:
            slice of list from percentile to end
        """
        self._sort()
        n = self.samples
        k = int(pcnt * (n + 1) / 100)
        i = min(max(k, 0), n)
        return self.population[:i]


class Server(object):
    """Yet another StatsD server.

    Collects events submitted by client applications via UDP. Events are
    collected and converted into aggregate carbon/graphite metric statements
    on a periodic basis and relayed to a carbon server.

    Adapted from statsd python port by https://github.com/sivy/py-statsd
    """

    def __init__(self, send_interval=60, pct_threshold=90,
            carbon_host='localhost', carbon_port=2004, prefix='',
            metric_host=None):
        """
        :param send_interval: How often to post metrics to graphite (seconds)
        :type send_interval: int
        :param pct_threshold: High percentile to track
        :type pct_threshold: int
        :param carbon_host: Hostname or ipv4 address to send metrics to
        :type carbon_host: string
        :param carbon_port: Carbon pickle receiving port
        :type carbon_port: int
        :param prefix: Prefix to add to all metrics
                       (should include trailing `.`)
        :type prefix: String
        :param metric_host: "Hostname" to report internal metrics from
        :type metric_host: string
        """
        self.log = logging.getLogger(self.__class__.__name__)
        self.max_dgram_size = 65507

        self.send_interval = send_interval
        self.pct_threshold = pct_threshold
        self.carbon_host = carbon_host
        self.carbon_port = carbon_port
        self.prefix = prefix

        if metric_host is None:
            metric_host = socket.gethostname()
        self.metric_host = metric_host.replace('.', '_')

        self.log.info('config: send_interval=%d, pct_threshold=%d, '
                'carbon_host=%s, carbon_port=%d, prefix=%s, metric_host=%s',
                self.send_interval, self.pct_threshold, self.carbon_host,
                self.carbon_port, self.prefix, self.metric_host)

        self.counters = None
        self.timers = None
        self.gauges = None
        self.sets = None

        self._reset_collections()
    # end __init__

    def _reset_collections(self):
        """
        Replace current collections with empty collections
        and return the current collections.

        :return: Tuple of `(counters, timers, gauges, sets)`
        :rtype: tuple
        """
        c = self.counters
        t = self.timers
        g = self.gauges
        s = self.sets

        self.counters = collections.defaultdict(int)
        self.timers = collections.defaultdict(Stats)
        self.gauges = collections.defaultdict(int)
        self.gauges.update(g or {})
        self.sets = collections.defaultdict(set)

        return (c, t, g, s)
    # end _reset_collections

    def _internal_metric(self, name):
        return '%syastatsd.%s.%s' % (self.prefix, self.metric_host, name)
    # end _internal_metric

    @staticmethod
    def _clean_metric(k):
        """Make an event key safe for use as a carbon metric path.

        Converts '/' to '-', ' ' to '_' and strips non-word chars from the key.

        :param k: Key to sanitize
        :type k: string
        :return: Sanitized key
        :rtype: string
        """
        return CLEAN_RE.sub('', k.replace('/', '-').replace(' ', '_'))
    # end _clean_metric

    def handle_message(self, payload):
        """Handle a message received from a client.

        Expected messages:
            - some.counter.metric:1|c
            - some.sampled.counter:1|c|@0.1
            - some.timing.metric:100|ms
            - some.gauge.metric:11|g
            - some.gauge.metric:+13|g
            - some.gauge.metric:-1|g
            - some.set.metric:42|s

        :param payload: Message from client
        :type payload: string
        """
        self.log.debug("handle: %s", payload)
        self.counters[self._internal_metric("recv")] += 1

        try:
            payload.rstrip('\n')

            for metric in payload.split('\n'):
                match = METRIC_RE.match(metric)

                if match is None:
                    self.log.error('Malformed metric <%s>', metric)
                    self.counters[self._internal_metric('errors')] += 1
                    continue

                # clean non-standard characters out of metric name
                name = match.group('name')
                key = self._clean_metric(name)

                if '' == key:
                    self.log.error('Metric name <%s> invalid in <%s>',
                        name, metric)
                    self.counters[self._internal_metric('errors')] += 1
                    continue

                value = match.group('value')
                opts = match.group('opts')
                mtype = opts.pop(0)

                if mtype == 'c':
                    self._record_counter(key, value, opts)
                elif mtype == 'ms':
                    self._record_timer(key, value, opts)
                elif mtype == 'g':
                    self._record_gauge(key, value, opts)
                elif mtype == 's':
                    self._record_set(key, value, opts)
                else:
                    self.log.error('Unknown metric type in <%s>', metric)
                    self.counters[self._internal_metric('errors')] += 1
                    continue

        except (KeyboardInterrupt, SystemExit), e:
            raise

        except Exception, e:
            self.log.exception('Error processing message: %s', e)
            self.counters[self._internal_metric('recv_errors')] += 1
    # end handle_message

    def _record_counter(self, key, value, opts):
        sample_rate = 1
        if opts:
            # get custom sample rate to adjust the value
            sample_rate = float(opts[0][1:])

        value = int(value) * (1 / sample_rate)
        self.log.debug('counter %s: %s', key, value)
        self.counters[key] += int(value)
    # end _record_counter

    def _record_timer(self, key, value, opts):
        self.log.debug('timer %s: %s', key, value)
        self.timers[key].record(int(value))
    # end _record_timer

    def _record_gauge(self, key, value, opts):
        if value[1] in ('-', '+'):
            # delta to existing value
            value = int(value) + self.gauges[key]
        self.log.debug('gauge %s: %s', key, value)
        self.gauges[key] = value
    # end _record_gauge

    def _record_set(self, key, value, opts):
        self.log.debug('set %s: %s', key, value)
        self.sets[key].add(value)
    # end _record_set

    def send_to_carbon(self):
        """
        Send all currently collected counters and timer information to carbon
        using the carbon pickle protocol.

        Counters send as a single data point per interval that is the
        arithmetic sum of all samples seen.

        Timers send in 6 data points per interval:
            - `count`: number of samples seen
            - `lower`: lowest sample seen
            - `upper`: highest sample seen
            - `stddev`: standard deviation of population
            - `p_N`: Nth percentile (configurable)
            - `mean`: arithmetic average of samples below Nth percentile

        Gauges send as a single data point per interval that is the current
        value of the gauge.

        Sets send as a single data point per interval that is the cardinality
        of the collection of values received.
        """
        self.log.debug('sending to carbon')
        data = []
        ts = int(time.time())
        metrics = 0
        counters, timers, gauges, sets = self._reset_collections()

        for key, val in counters.items():
            # send counters as-is
            data.append(('%s%s' % (self.prefix, key), (ts, val)))
            metrics += 1

        for key, stats in timers.items():
            metric = '%s%s' % (self.prefix, key)
            if stats.samples > 0:
                data.append(('%s.count' % metric, (ts, stats.samples)))
                data.append(('%s.lower' % metric, (ts, stats.min)))
                data.append(('%s.upper' % metric, (ts, stats.max)))
                data.append(('%s.stdev' % metric, (ts, stats.stdev)))
                data.append(('%s.p_%d' % (metric, self.pct_threshold),
                    (ts, stats.percentile(self.pct_threshold))))

                # rather than raw mean, we record the mean of the set below
                # the percentile cuttoff
                pop = stats.below_percentile(self.pct_threshold)
                data.append(('%s.mean' % metric,
                    (ts, sum(pop) / max(1, len(pop)))))

                metrics += 1

        for key, val in gauges.items():
            # send gauges as-is
            data.append(('%s%s' % (self.prefix, key), (ts, val)))
            metrics += 1

        for key, val in sets.items():
            # send number of items in set
            data.append(('%s%s' % (self.prefix, key), (ts, len(val))))
            metrics += 1

        data.append((self.prefix + self._internal_metric('metrics'),
                (ts, metrics)))
        self.log.info('Found %d metrics to send to carbon', metrics)

        try:
            sock = socket.create_connection(
                (self.carbon_host, self.carbon_port), 1)

            # send in batches of 500 metrics or less
            for x in range(0, len(data), 500):
                # binary pickle
                payload = pickle.dumps(data[x:x + 500], protocol=-1)
                payload_len = struct.pack('!L', len(payload))
                message = payload_len + payload
                self.log.info('Sending %d bytes to carbon', len(message))
                sock.sendall(message)
            # end for

            sock.shutdown(socket.SHUT_RDWR)
            sock.close()

        except socket.error, msg:
            self.log.exception('Socket error sending to carbon: %s', msg)
            self.counters[self._internal_metric('send_errors')] += 1
    # end send_to_carbon

    def _timer_task(self):
        """
        Periodic task to run via threading.Timer that will send collected
        metrics to Carbon and reschedule itself to run again.

        Any unexpected errors from send_to_carbon() will be logged and then
        the whole server will be stopped.
        """
        try:
            self.send_to_carbon()
        except Exception, e:
            self.log.exception('Unexpected error sending to carbon: %s', e)
            # treat as fatal
            self.stop()

        if self._keep_running():
            # reset the timer
            self._set_timer()
    # end _timer_task

    def _set_timer(self):
        """
        Set a timer that will send accumulated metrics to carbon.
        """
        self._timer = threading.Timer(self.send_interval, self._timer_task)
        self._timer.start()
    # end _set_timer

    def _keep_running(self):
        return self._sock is not None
    # end _keep_running

    def serve(self, hostname='', port=8125):
        """
        Listen for UDP datagrams sent to the given host and port.
        Process each datagram as it arrives.

        :param hostname: Hostname or ipv4 dotted quad address
        :type hostname: string
        :param port: UDP port number
        :type port: int
        """
        self.log.info('Starting server on %s:%d', hostname, port)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((hostname, port))

        # trap some os signals
        import signal

        def stop_on_signal(signal, frame):
            self.log.warning('Received signal %d', signal)
            self.stop()
        # end stop_on_signal

        signal.signal(signal.SIGINT, stop_on_signal)
        signal.signal(signal.SIGHUP, stop_on_signal)
        signal.signal(signal.SIGTERM, stop_on_signal)

        self._set_timer()

        while self._keep_running():
            try:
                self.log.debug('waiting for dgram')
                payload, addr = self._sock.recvfrom(self.max_dgram_size)
                self.log.info('dgram from %s', addr)
                self.handle_message(payload)

            except (KeyboardInterrupt, SystemExit), e:
                self.log.warning('Exiting: %s', e)
                raise

            except Exception, e:
                self.log.exception('Error serving: %s ', e)
                self.counters[self._internal_metric('serv_errors')] += 1

        # flush any partial stats that are laying around to carbon
        self.send_to_carbon()
    # end serve

    def stop(self):
        """
        Cancel timer and close socket on shutdown
        """
        self.log.warning('shutting down')
        self._timer.cancel()
        self._sock.close()
        self._sock = None
    # end stop
# end class Server


def run_server():
    # parse arguments
    import optparse
    parser = optparse.OptionParser(usage='usage: %prog [options]')
    parser.add_option('-c', '--conf',
        dest='conf', default='yastatsd.cfg',
        help='Path to configuration file')
    (opt, args) = parser.parse_args()

    # read config file
    import ConfigParser
    conf = ConfigParser.ConfigParser()
    conf.readfp(open(opt.conf))

    # setup logger
    if conf.has_option('files', 'logging'):
        import os
        os.environ['TZ'] = 'Etc/UTC'
        time.tzset()
        import logging.config
        logging.config.fileConfig(open(conf.get('files', 'logging')))
        logging.Formatter.converter = time.localtime

    # start server
    server = Server(
        carbon_host=conf.get('carbon', 'host'),
        carbon_port=conf.getint('carbon', 'port'),
        send_interval=conf.getint('carbon', 'interval'),
        pct_threshold=conf.getint('metrics', 'percentile'),
        prefix=conf.get('metrics', 'prefix')
    )

    server.serve(conf.get('listen', 'host'), conf.getint('listen', 'port'))

if __name__ == '__main__':
    run_server()
