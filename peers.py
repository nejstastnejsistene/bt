import random
import select
import socket
import struct
import threading
import time

import bencode
import bitfield
import rates

names = ['choke','unchoke','interested','uninterested',
         'have','bitfield','request','piece','cancel']

handshake_protocol = chr(19) + 'BitTorrent protocol'

reserved_bits = chr(0)*8
##reserved_bits[5] |= 0x10 #
##reserved_bits[7] |= 0x04 # fast ext
##reserved_bits[7] |= 0x01 #

choke_id =          0x00
unchoke_id =        0x01
interested_id =     0x02
uninterested_id =   0x03
have_id =           0x04
bitfield_id =       0x05
request_id =        0x06
piece_id =          0x07
cancel_id =         0x08

suggest_piece_id =  0x0D 
have_all_id =       0x0E
have_none_id =      0x0F
reject_request_id = 0x10
allowed_fast_id =   0x11

ltep_id =           0x14
ltep_handshake_id = 0x00

block_size = 2**14


class PeerManager:

    def __init__(self, metainfo, peer_id, bitfield, pieceio, update_rates,
                 update_udl, piece_completed, reannounce):
        self.metainfo = metainfo
        self.peer_id = peer_id
        self.bitfield = bitfield
        self.pieceio = pieceio
        self.update_rates = update_rates
        self.update_udl = update_udl
        self.piece_completed = piece_completed
        self.reannounce = reannounce
        self.num_blocks = self.metainfo.info.piece_size / block_size
        self.pieces = []
        for i in range(self.metainfo.info.num_pieces):
            self.pieces.append([0]*self.num_blocks)
        self._rarity = []
        self.unchoked = []
        self.peers_unchoked = []
        self.interested = []
        self.peers_interested = []
        self.downloading = []
        self.uploading = []
        self.fast_allowed = []
        self.connections = []
        self.conns_lock = threading.Lock()
        self.reannounced = False
        self.start()

    def start(self):
        self.running = True
        threading.Thread(target=self.unchoker).start()
        threading.Thread(target=self.requester).start()

    def stop(self):
        self.running = False
        self.drop_all()

    def add_peer_by_info(self, peer_info):
        addr = peer_info['ip'], peer_info['port']
        self.add_peer(addr, peer_info['peer id'])

    def add_peer(self, addr, peer_id, sock=None):
        peer = Peer(addr, peer_id, self.metainfo.info.num_pieces)
        conn = PeerConn(peer, sock, self.metainfo.info_hash,
            self.peer_id, self.bitfield, self.drop_connection,
            self.update_rates, self.update_udl, self.handle_msg)
        with self.conns_lock:
            self.connections.append(conn)
        threading.Thread(target=self.connect_peer, args=(conn,)).start()

    def connect_peer(self, conn):
        try:
            conn.connect()
            if conn.fastext_enabled:
                for i in fast_allowed:
                    conn.send_fast_allowed(i)
        except DropConnection:
            pass

    def drop_connection(self, conn):
        with self.conns_lock:
            conn.close()
            if conn in self.connections:
                self.connections.remove(conn)
            if not self.reannounced and len(self.connections) < 10 and \
                   self.running:
                self.reannounced = True
                self.reannounce()

    def drop_all(self):
        while self.connections:
            self.drop_connection(self.connections[0])

    def handle_msg(self, conn, msg_id, *args):
        if msg_id == choke_id or msg_id == unchoke_id:
            self.handle_choke(conn, *args)
        elif msg_id == interested_id or msg_id == uninterested_id:
            self.handle_interested(conn, *args)
        elif msg_id == have_id or msg_id == bitfield_id:
            self.update_peer_interest(conn)
        elif msg_id == request_id:
            self.handle_request(conn, *args)
        elif msg_id == piece_id:
            self.handle_piece(conn, *args)

    def handle_choke(self, conn, flag):
        if not flag and not conn in self.peers_unchoked:
            self.peers_unchoked.append(conn)
        elif flag and conn in self.peers_unchoked:
            self.peers_unchoked.remove(conn)

    def handle_interested(self, conn, flag):
        if flag and not conn in self.peers_interested:
            self.peers_interested.append(conn)
        elif not flag and conn in self.peer_interested:
            self.peers_interested.remove(conn)

    def update_interested(self):
        counts = [0]*self.metainfo.info.num_pieces
        with self.conns_lock:
            for conn in self.connections:
                bf = conn.peer.bitfield
                for i in range(len(bf)):
                    counts[i] += bf[i]
        rarity = []
        i = 0
        for n in sorted(set(counts)):
            tier = []
            for j in range(len(counts)):
                if counts[j] == n:
                    tier.append(j)
            rarity.append(tier)
        self._rarity = rarity
        with self.conns_lock:
            for conn in self.connections:
                self.update_peer_interest(conn)
        self.interested = sorted(self.interested, key=PeerConn.interest)

    def update_peer_interest(self, conn):
        interested = False
        for i in range(len(self.bitfield)):
            if conn.peer.bitfield[i] and not self.bitfield[i]:
                interested = True
        if interested and not conn in self.interested:
            conn.send_interested()
            self.interested.append(conn)
        elif not interested and conn in self.interested:
            conn.send_uninterested()
            self.interested.pop(conn)
        conn.update_interest(self.rarity)

    def rarity(self, index):
        for tier in self._rarity:
            if index in tier:
                return self._rarity.index(tier)
        
    def handle_request(self, conn, index, begin, length):
        if not self.bitfield[index]:
            reject = True
        elif conn.peer.bitfield[i]:
            reject = True
        elif peer.fastext_enabled and index in self.fast_allowed:
            reject = False
        elif not self.allow_transfer(conn):
            reject = True
        if reject:
            if conn.fastext_enabled:
                conn.send_reject_request(index, begin, length)
        else:
            threading.Thread(target=self.fulfill_request,
                args=(conn, index, begin, length)).start()

    def allow_transfer(self, conn):
        return conn in self.peers_unchoked and conn in self.unchoked and \
            (conn in self.peers_interested or conn in self.interested)

    def fulfill_request(self, conn, index, begin, length):
        piece = self.pieceio.read_piece(index, begin, length)
        conn.send_piece(index, begin, piece)
            
    def handle_piece(self, conn, index, begin, piece):
        threading.Thread(target=self._handle_piece,
            args=(conn, index, begin, piece)).start()

    def _handle_piece(self, conn, index, begin, piece):
        if self.pieces[index][begin/block_size]:
            self.update_udl(wasted=len(piece))
        else:
            self.pieceio.write_piece(index, begin, piece)
            self.pieces[index][begin/block_size] = 1
            if all(self.pieces[index]):
                if not self.piece_completed(index):
                    self.pieces[index] = [0]*self.num_blocks
                else:
                    with self.conns_lock:
                        for conn in self.connections:
                            if conn.connected:
                                conn.send_have(index)

    def unchoker(self):
        counter = 0
        while self.running:
            self.update_interested()
            # evaluate uloaders/dloaders
            if not counter:
                pass # optimistic unchoke
            counter = (counter + 1) % 3
            time.sleep(10)

    def choke_peer(self, conn):
        conn.send_choke()
        self.unchoked.remove(conn)
        
    def unchoke_peer(self, conn):
        conn.send_unchoke()
        self.unchoked.append(conn)

    def requester(self):
        pass # decides when/how/what pieces to request

    def tick_rates(self):
        with self.conns_lock:
            for conn in self.connections:
                conn.tick_rates()


class PeerConn:

    def __init__(self, peer, sock, info_hash, peer_id, bitfield,
                 disconnect, update_rates, update_udl, handle_msg):
        self.peer = peer
        self.sock = sock
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.bitfield = bitfield
        self._disconnect = disconnect
        self._update_rates = update_rates
        self.ud_rates = rates.TransferRates(20)
        self.queue = RequestQueue(self._send_request)
        self.update_udl = update_udl
        self.handle_msg = handle_msg
        self.interest = 0
        self.connected = False

    def update_interest(self, rarity):
        interest = 0
        for i in range(len(self.peer.bitfield)):
            if self.peer.bitfield[i] and not self.bitfield[i]:
                interest += rarity(i)
        self.interest = interest

    def interest(self):
        return self.interest

    def _recv(self, size):
        data = self.sock.recv(size)
        self.update_rates(down=len(data))
        return data

    def recv(self, size):
        data = self._recv(size)
        while len(data) != size:
            data += self._recv(size - len(data))
        return data

    def send(self, data):
        try:
            self.sock.send(data)
        except socket.error as err:
            if err.errno == 10054:
                self.disconnect()
        self.update_rates(up=len(data))

    def update_rates(self, up=0, down=0):
        self.ud_rates.update(up, down)
        self._update_rates(up, down)

    def tick_rates(self):
        self.ud_rates.tick()
        qsize = 2 + int(self.ud_rates.down() / 10000)
        if qsize != self.queue.size:
            self.queue.resize(qsize)

    def connect(self):
        try:
            if self.sock:
                self.recv_handshake()
                self.send_handshake()
            else:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect(self.peer.addr)
                self.send_handshake()
                self.recv_handshake()
            if self.ltep_enabled:
                self.send_ltep_handshake()
            if self.fastext_enabled:
                if all(self.bitfield):
                    self.send_have_all()
                elif not any(self.bitfield):
                    self.send_have_none()
            elif any(self.bitfield):
                self.send_bitfield()
        except socket.error:
            self.disconnect()
        self.connected = True
        threading.Thread(target=self.recv_loop).start()

    def disconnect(self):
        self._disconnect(self)
        raise DropConnection

    def recv_handshake(self):
        handshake = self.recv(68)
        if handshake[:20] != handshake_protocol:
            self.disconnect()
        elif handshake[28:48] != self.info_hash:
            self.disconnect()
        elif self.peer.peer_id is None:
            self.peer.peer_id = handshake[48:68]
        elif  handshake[48:68] != self.peer.peer_id:
            self.disconnect()
        reserved = handshake[20:28]
        self.ltep_enabled = ord(reserved[5]) & 0x10 and False
        self.dht_enabled = ord(reserved[7]) & 0x01 and False
        self.fastext_enabled = ord(reserved[7]) & 0x04 and False

    def send_handshake(self):
        self.send(handshake_protocol)
        self.send(reserved_bits)
        self.send(self.info_hash)
        self.send(self.peer_id)

    def recv_loop(self):
        self.running = True
        while self.running:
            try:
                select.select([self.sock], [], [])
                self.recv_msg()
            except DropConnection:
                break
        self.sock.close()

    def recv_msg(self):
        try:
            length, = struct.unpack('!I', self.recv(4))
            if not length:
                print 'msg received: keep alive\n',
                return
            msg_id = ord(self.recv(1))
            if msg_id not in [4, 5]:
                try:
                    print 'msg received: %s\n'% names[msg_id],
                except IndexError:
                    print msg_id
            if length > 1:
                payload = self.recv(length - 1)
            if msg_id == choke_id:
                self.handle_choke()
            elif msg_id == unchoke_id:
                self.handle_unchoke()
            elif msg_id == interested_id:
                self.handle_interested()
            elif msg_id == uninterested_id:
                self.handle_uninterested()
            elif msg_id == have_id:
                self.handle_have(payload)
            elif msg_id == bitfield_id:
                self.handle_bitfield(payload)
            elif msg_id == request_id:
                self.handle_request(payload)
            elif msg_id == piece_id:
                self.handle_piece(payload)
            elif msg_id == cancel_id:
                self.handle_cancel(payload)
            elif self.fastext_enabled and msg_id == suggest_piece_id:
                pass
            elif self.fastext_enabled and msg_id == have_all_id:
                pass
            elif self.fastext_enabled and msg_id == have_none_id:
                pass
            elif self.fastext_enabled and msg_id == reject_request_id:
                pass
            elif self.fastext_enabled and msg_id == fast_allowed_id:
                pass
            elif self.ltep_enabled and msg_id == ltep_id:
                pass
            else:
                self.disconnect()
        except socket.error:
            self.disconnect()

    def handle_choke(self):
        self.choked = True
        self.handle_msg(self, choke_id, True)

    def handle_unchoke(self):
        self.choked = False
        self.handle_msg(self, unchoke_id, False)

    def handle_interested(self):
        self.handle_msg(self, interested_id, True)

    def handle_uninterested(self):
        self.handle_msg(self, uninterested_id, False)

    def handle_have(self, payload):
        index, = struct.unpack('!I', payload)
        try:
            self.peer.bitfield[index] = 1
            self.handle_msg(self, have_id, False)
        except IndexError:
            self.disconnect()

    def handle_bitfield(self, payload):
        try:
            self.peer.bitfield.unpack(payload)
            self.handle_msg(self, bitfield_id, False)
        except ValueError:
            self.disconnect()

    def handle_request(self, payload):
        index, begin, length = struct.unpack('!III', payload)
        self.handle_msg(self, request_id, index, begin, length)

    def handle_piece(self, payload):
        index, begin = struct.unpack('!II', payload[:8])
        piece = payload[8:]
        self.update_udl(down=len(piece))
        self.queue.pop(index, begin)
        self.handle_msg(self, piece_id, index, begin, piece)
        
    def handle_cancel(self, payload):
        index, begin, length = struct.unpack('!III', payload)

    def handle_ltep(self, payload):
        pass

    def send_int(self, i):
        self.send(struct.pack('!I', i))

    def send_choke(self):
        self.send_int(1)
        self.send(chr(choke_id))

    def send_unchoke(self):
        self.send_int(1)
        self.send(chr(unchoke_id))

    def send_interested(self):
        self.send_int(1)
        self.send(chr(interested_id))

    def send_uninterested(self):
        self.send_int(1)
        self.send(chr(uninterested_id))

    def send_have(self, index):
        self.send_int(5)
        self.send(chr(have_id))
        self.send_int(index)

    def send_bitfield(self, bs=None):
        if bs is None:
            bs = self.bitfield.pack()
        self.send_int(1 + len(bs))
        self.send(chr(bitfield_id))
        self.send(bs)

    def send_lazy_bitfield(self, num_haves=20):
        bf = self.bitfield.clone()
        pieces = range(len(bf))
        random.shuffle(pieces)
        haves = []
        for i in pieces:
            if bf[i]:
                bf[i] = 0
                haves.append(i)
                if len(haves) > num_haves:
                    break
        self.send_bitfield(bf.pack())
        for i in haves:
            self.send_have(i)

    def send_request(self, index, begin, length):
        self.queue.push(index, begin, length)

    def _send_request(self, index, begin, length):
        self.send_int(13)
        self.send(chr(request_id))
        self.send_int(index)
        self.send_int(begin)
        self.send_int(length)

    def send_piece(self, index, begin, piece):
        self.send_int(9 + len(piece))
        self.send(chr(piece_id))
        self.send_int(index)
        self.send_int(begin)
        self.send(piece)
        self.update_udl(up=length)

    def send_cancel(self, index, begin, length):
        self.send_int(13)
        self.send(chr(cancel_id))
        self.send_int(index)
        self.send_int(begin)
        self.send_int(length)

    def send_suggest_piece(self, index):
        self.send_int(5)
        self.send(chr(suggest_piece_id))
        self.send_int(index)

    def send_have_all(self):
        self.send_int(1)
        self.send(chr(have_all_id))

    def send_have_none(self):
        self.send_int(1)
        self.send(chr(have_none_id))

    def send_reject_request(self, index, begin, length):
        self.send_int(13)
        self.send(chr(reject_request_id))
        self.send_int(index)
        self.send_int(begin)
        self.send_int(length)

    def send_allowed_fast(self, index):
        self.send_int(5)
        self.send(chr(allowed_fast_id))
        self.send_int(index)

    def send_ltep_handshake(self):
        pass
        # payload = {
        #     'm': {'name': msg_id, 'name2': msg_id2,},
        #     'p': port_num,
        #     'v': 'client_name',}
        # }
        # payload = bencode.bencode(payload)
        # fmt = '!Icc%ds' % len(payload)
        # self.send(struct.pack(fmt, 6 + len(payload), chr(ltep_id), chr(ltep_handshake_id), payload))

    def close(self):
        self.running = False
     

class RequestQueue:

    def __init__(self, send_request):
        self.send_request = send_request
        self.pending = []
        self.queue = []
        self.size = 2
        self.lock = threading.Lock()

    def push(self, index, begin, length):
        with self.lock:
            if len(self.pending) < self.size:
                self.send_request(index, begin, length)
                self.pending.append((index, begin, length))
            else:
                self.queue.append((index, begin, length))

    def pop(self, index, begin):
        with self.lock:
            ret = False
            for req in self.pending:
                if req[:2] == (index, begin):
                    self.pending.remove(req)
                    ret = True
            self.flush()
        return ret

    def flush(self):
        while len(self.pending) < self.size and self.queue:
            req = self.queue.pop(0)
            self.send_request(*req)
            self.pending.append(req)
            
    def resize(self, size):
        self.size = size
        self.flush()

    def cancel(self, index, begin, length):
        with self.lock:
            if not self.pop(index, begin):
                for req in self.queue:
                    if req == (index, begin, length):
                        self.queue.remove(req)


class Peer:

    def __init__(self, addr, peer_id, num_pieces):
        self.addr = addr
        self.peer_id = peer_id
        self.bitfield = bitfield.Bitfield(num_pieces)


class DropConnection(Exception):
    pass
