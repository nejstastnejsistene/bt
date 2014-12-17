import errno
import httplib
import random
import socket
import struct
import time
import threading
import urllib
import urlparse

import bencode
import peers

#############################################################################
## store data about seeders, leechers, and completed downloads somewhere :D #
## and also prevent announces/scrapes if a minimum interval has been set    #
#############################################################################

class Announcer:

    def __init__(self, metainfo, peer_id, port, udl, callback):
        if 'announce-list' in metainfo:
            self.trackers = metainfo['announce-list']
        elif 'announce' in metainfo:
            self.trackers = metainfo['announce']
        for i in range(len(self.trackers)):
            if isinstance(self.trackers[i], list):
                random.shuffle(self.trackers[i])
        self.rankings = range(len(self.trackers))[::-1]
        self.pending_requests = []
        self.info_hash = metainfo.info_hash
        self.peer_id = peer_id
        self.key = random.getrandbits(32)
        self.tracker_id = None
        self.port = port
        self.sched = []
        self.sleeper = threading.Condition()
        self.sched_lock = threading.Lock()
        self.udl = udl
        self.callback = callback        

    def start(self):
        self.running = True
        threading.Thread(target=self.schedule_loop).start()
        self.reannounce(1)
        
    def schedule_loop(self):
        while self.running:
            with self.sched_lock:
                wait_time = None
                for event in self.sched:
                    t, args = event
                    if time.time() >= t:
                        self.sched.remove(event)
                        self.request(*args)
                    elif not wait_time or t - time.time() < wait_time:
                        wait_time = t - time.time()
            with self.sleeper:
                self.sleeper.wait(wait_time)

    def stop(self):
        with self.sched_lock:
            self.sched = []
            self.running = False
            with self.sleeper:
                self.sleeper.notify()

    def schedule(self, t, announce, tracker, event):
        with self.sched_lock:
            self.sched.append((time.time() + t,
                (announce, tracker, event)))
        with self.sleeper:
            self.sleeper.notify()

    def unschedule(self, announce, tracker):
        with self.sched_lock:
            for event in self.sched:
                if event[1][:2] == (announce, tracker):
                    self.sched.remove(event)

    def is_scheduled(self, announce, tracker):
        with self.sched_lock:
            for event in self.sched:
                if event[1][:2] == (announce, tracker):
                    return True
        
    def do_callback(self, tracker, tracker_info, announce):
        interval = 3600
        if 'min_request_interval' in tracker_info:
            interval = tracker_info['min_request_interval']
        if 'min_interval' in tracker_info:
            interval = tracker_info['min_interval']
        if 'interval' in tracker_info:
            interval = tracker_info['interval']
        if not self.is_scheduled(announce, tracker):
            self.schedule(interval, announce, tracker, 0)
        if 'tracker_id' in tracker_info:
            self.tracker_id = tracker_info['tracker_id']
        if 'completed' in tracker_info and 'incomplete' in tracker_info:
            seeders = tracker_info['completed']
            leechers = tracker_info['incomplete']
            self.set_rank(tracker, 2 * seeders + leechers)
        self.callback(tracker, tracker_info)

    def get_tracker_index(self, tracker):
        for tier in self.trackers:
            if tracker == tier or tracker in tier:
                return self.trackers.index(tier)

    def get_rank(self, tracker):
        return self.rankings[self.get_tracker_index(tracker)]

    def set_rank(self, tracker, rank):
        index = self.get_tracker_index(tracker)
        if rank < 0 and isinstance(self.trackers[index], list):
            new_tier = self.trackers[index]
            new_tier.remove(tracker)
            new_tier.append(tracker)
            self.trackers[index] = new_tier
        self.rankings[index] = rank

    def sorted_trackers(self):
        return sorted(self.trackers, lambda a, b: cmp(self.get_rank(a),
            self.get_rank(b)), reverse=True)
    
    def reannounce(self, event):
        self.announce_all(event)
        self.scrape_all()

    def announce_all(self, event):
        self.request_all(True, event)

    def scrape_all(self):
        self.request_all(False)

    def request_all(self, announce, event=False):
        threading.Thread(target=self._request_all,
            args=(announce, event)).start()
       
    def _request_all(self, announce, event):
        sorted_trackers = self.sorted_trackers()
        for i in range(len(sorted_trackers)):
            tier = sorted_trackers[i]
            if isinstance(tier, str):
                self.request(announce, tier, event)
            elif isinstance(tier, list):
                self.request(announce, tier[0], event)
            time.sleep(10)

    def request(self, announce, tracker, event):
        threading.Thread(target=self._request,
            args=(announce, tracker, event)).start()

    def _request(self, announce, tracker, event):
        if tracker in self.pending_requests:
            self.unschedule(announce, tracker)
            self.schedule(5, announce, tracker, event)
            return
        self.pending_requests.append(tracker)
        scheme = tracker.split('://')[0]
        nil, netloc, path, nil, nil, nil = \
            urlparse.urlparse(tracker.replace(scheme, 'http'))
        port = None
        if ':' in netloc:
            netloc, port = netloc.split(':')
            port = int(port)
        try:
            if scheme == 'http' or scheme == 'https':
                if announce:
                    tracker_info = self.request_tracker(True, True, scheme,
                        netloc, port, path, self.info_hash, self.peer_id,
                        self.port, self.udl(), event, self.key,
                        -1, self.tracker_id)
                else:
                    tracker_info = self.request_tracker(True, False, scheme,
                        netloc, port, path, self.info_hash)
            elif scheme == 'udp':
                if announce:
                    tracker_info = self.request_tracker(False, True, netloc,
                        port, self.info_hash, self.peer_id, self.udl(), event,
                        0, self.key, -1, self.port)
                else:
                    tracker_info = self.request_tracker(False, False, netloc,
                        port, self.info_hash)
            else:
                raise BadTrackerError, '"%s" scheme not supported' % scheme
            self.do_callback(tracker, tracker_info, announce)
        except BadTrackerError:
            if not self.is_scheduled(announce, tracker):
                self.schedule(3600, announce, tracker, event)
            self.set_rank(tracker, -1)
        self.pending_requests.remove(tracker)
    
    def request_tracker(self, http, announce, *args):
        conn = None
        try:
            if http:
                cl = HTTPTrackerConnection
            else:
                cl = UDPTrackerConnection
            i = len(cl.__init__.func_code.co_varnames) - 1
            conn = cl(*args[:i])
            if announce:
                tracker_info = conn.announce(*args[i:])
            else:
                tracker_info = conn.scrape(*args[i:])
            conn.close()
            return tracker_info
        except socket.timeout:
            raise BadTrackerError, 'request timed out'
        except socket.gaierror:
            raise BadTrackerError, 'host not found'
        except socket.error as err:
            if err.errno == errno.ECONNRESET:
                raise BadTrackerError, 'connection reset'
            elif err.errno == errno.ETIMEDOUT:
                raise BadTrackerError, 'request timed out'
            elif err.errno == errno.ESHUTDOWN:
                raise AnnounceError, 'request was cancelled by client'
            elif err.errno == errno.ECONNREFUSED:
                raise BadTrackerError, 'connection refused'
            else:
                raise err
        except UDPError:
            raise BadTrackerError('bad packet recieved')
        finally:
            if conn:
                conn.close()
        

class HTTPTrackerConnection:

    def __init__(self, scheme, host, peer_port, path):
        if scheme == 'http':
            if peer_port is None:
                peer_port = httplib.HTTP_PORT
            self.conn = httplib.HTTPConnection(host, peer_port, timeout=120)
        elif scheme == 'https':
            if peer_port is None:
                peer_port = httplib.HTTPS_PORT
            self.conn = httplib.HTTPSConnection(host, peer_port, timeout=120)
        self.path = path
        self.sock = self.conn.sock

    def announce(self, info_hash, peer_id, port, udl, event, key,
                 num_want, tracker_id):
        query = '?info_hash=%s&peer_id=%s&port=%d' % \
            (urllib.quote(info_hash), urllib.quote(peer_id), port)
        query += '&uploaded=%d&downloaded=%d&left=%d&compact=1' % udl
        if event:
            query += ['started', 'stopped', 'complete'][event - 1]
        query += '&key=%d' % key
        if num_want > 0:
            query += '&num_want=%d' % num_want
        if tracker_id:
            query += '&trackerid=%s' % tracker_id
        self.conn.request('GET', self.path + query)
        response = self.conn.getresponse().read()
        response = self._verify_response(response)
        if isinstance(response['peers'], str):
            response['peers'] = _convert_peers(response['peers'])
        return response

    def scrape(self, info_hash):
        i = self.path.rfind('/') + 1
        if self.path[i:i + 8] == 'announce':
            self.path = self.path[:i] + 'scrape' + self.path[i + 8:]
        else:
            raise BadTrackerError, 'scrape not supported'
        query = '?info_hash=%s' % urllib.quote(info_hash)
        self.conn.request('GET', self.path + query)
        response = self.conn.getresponse().read()
        response = self._verify_response(response)
        flags = {}
        if 'flags' in response:
            flags = response['flags']
        tracker_info = response['files'][info_hash]
        tracker_info.update(flags)
        return tracker_info

    def _verify_response(self, response):
        try:
            response = bencode.bdecode(response)
        except ValueError:
            raise BadTrackerError, 'response not bencoded'
        if 'failure reason' in response:
            raise BadTrackerError, response['failure reason']
        return response

    def close(self):
        self.conn.close()


class UDPTrackerConnection:

    def __init__(self, host, peer_port):
        if peer_port == None:
            raise BadTrackerError, 'no port given'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.connect((host, peer_port))
        self.connect()

    def _request(self, func, args=[], try_num=0):
        try:
            self.sock.settimeout(15 * 2 ** try_num)
            trans_id = random.getrandbits(32)
            return func(*(list(args) + [trans_id]))
        except socket.timeout:
            if try_num >= 2:
                raise
            else:
                return self._request(func, args, try_num + 1)

    def connect(self):
        self._request(self._connect)

    def _connect(self, trans_id):
        packet = 0x41727101980, 0, trans_id
        packet = struct.pack('!QII', *packet)
        self.sock.send(packet)
        packet = self.sock.recv(32)
        self._verify_packet(packet, 16, 0, trans_id)
        packet = self._unpack('!IIQ', packet)
        self.conn_id = packet[2]
        self.expires = time.time() + 60

    def announce(self, *args):
        return self._request(self._announce, args)

    def _announce(self, info_hash, peer_id, udl, event, ip, key, num_want,
            port, trans_id):
        self._try_reconnect()
        packet = (self.conn_id, 1, trans_id, info_hash, peer_id) + \
            udl + (event, ip, key, num_want, port)
        packet = struct.pack('!QII20s20sQQQIIIiH', *packet)
        self.sock.send(packet)
        packet = self.sock.recv(1024)
        self._verify_packet(packet, 20, 1, trans_id)
        peers_len = len(packet) - 20
        packet = self._unpack('!IIIII%ds' % peers_len, packet)
        tracker_info = {
            'interval': packet[2],
            'incomplete': packet[3],
            'completed': packet[4],
            'peers': _convert_peers(packet[5])
        }
        return tracker_info

    def scrape(self, info_hash):
        return self._request(self._scrape, [info_hash])

    def _scrape(self, info_hash, trans_id):
        self._try_reconnect()
        packet = self.conn_id, 2, trans_id, info_hash
        packet = struct.pack('!QII20s', *packet)
        self.sock.send(packet)
        packet = self.sock.recv(32)
        self._verify_packet(packet, 8, 2, trans_id)
        packet = self._unpack('!IIIII', packet)
        tracker_info = {
            'completed': packet[2],
            'downloaded': packet[3],
            'incomplete': packet[4],
        }
        return tracker_info

    def _try_reconnect(self):
        if time.time() >= self.expires:
            self.connect()

    def _unpack(self, fmt, packet):
        packet = packet[:struct.calcsize(fmt)]
        return struct.unpack(fmt, packet)

    def _verify_packet(self, packet, min_len, action, trans_id):
        if len(packet) < min_len:
            raise UDPError, 'incomplete packet'
        header = self._unpack('!II', packet)
        if header[1] != trans_id:
            raise UDPError, 'action mismatch'
        elif header[0] == 3:
            msg_len = len(packet) - 8
            nil, nil, err_msg = self._unpack('II%ds' % msg_len, packet)
            raise BadTrackerError(err_msg)
        elif header[0] != action:
            raise UDPError, 'trans_id mismatch'

    def close(self):
        self.sock.close()


def _convert_peers(compact):
        peers_list = []
        for i in range(0, len(compact), 6):
            ip, port = struct.unpack('!4sH', compact[i:i+6])
            ip = '.'.join([str(ord(byte)) for byte in ip])
            peers_list.append({
                'ip': ip,
                'port': port,
                'peer id': None,
            })
        return peers_list


class AnnounceError(Exception):
    pass

class BadTrackerError(AnnounceError):
    pass

class UDPError(AnnounceError):
    pass

