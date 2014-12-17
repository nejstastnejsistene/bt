import random
import socket
import threading
import time

import announce
import metainfo
import peers
import pieceio
import rates

bleh = False
class BitTorrentClient:

    def __init__(self, metainfo_file, dl_dir=['downloads']):
        self.metainfo = metainfo.MetaInfo(metainfo_file)
        self.pieceio = pieceio.PieceIO(self.metainfo.info, dl_dir)
        self.peer_id = ''.join([chr(random.getrandbits(8)) for i in range(20)])
        self.udl = [0, 0, self.pieceio.data_left()]
        self.udl_lock = threading.Lock()
        self.ud_rates = rates.TransferRates(3600)
        self.tracker = None
        self.create_server()
        self.peer_manager = peers.PeerManager(self.metainfo,
            self.peer_id, self.pieceio.bitfield, self.pieceio,
            self.ud_rates.update, self.update_udl, self.piece_completed,
            self.reannounce)
        self.announcer = announce.Announcer(self.metainfo, self.peer_id,
            self.port, self.get_udl, self.tracker_callback)
        self.start()

    def start(self):
        self.announcing = True
        self.announcer.start()
        self.running = True
        threading.Thread(target=self.rates_ticker).start()
        threading.Thread(target=self.accept_peers).start()

    def pause(self):
        self.stop(True)

    def stop(self, pause=False):
        if pause:
            self.announcer.reannounce(2)
        else:
            self.announcer.stop()
        self.peer_manager.stop()
        self.running = False

    def create_server(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.port = 6881
        while self.port < 6890:
            try:
                self.server.bind(('localhost', self.port))
                self.server.listen(1)
                return
            except socket.error as err:
                if err.errno in (10013, 10048) and self.port != 6889:
                    self.port += 1
                else:
                    raise

    def reannounce(self, event=0):
        self.announcing = True
        self.announcer.reannounce(event)

    def accept_peers(self):
        while self.running:
            conn, addr = self.server.accept()
            print 'peer accepted', conn, addr
            self.peer_manager.add_peer(addr, None, conn)

    def update_tracker(self):
        self.announcer.reannounce()

    def tracker_callback(self, tracker, tracker_info):
        if bleh:
            return
        if 'peers' in tracker_info and self.announcing:
            self.announcing = False
            self.peer_manager.drop_all()
            self.tracker = tracker
            print 'using tracker %s'%tracker
            for peer_info in tracker_info['peers']:
                self.peer_manager.add_peer_by_info(peer_info)

    def update_udl(self, up=0, down=0, wasted=0):
        with self.udl_lock:
            self.udl[0] += up
            self.udl[1] += down
            self.udl[2] -= down + wasted

    def get_udl(self):
        with self.udl_lock:
            return tuple(self.udl)

    def rates_ticker(self):
        while self.running:
            self.ud_rates.tick()
            self.peer_manager.tick_rates()
            time.sleep(1)

    def piece_completed(self, index):
        success = self.pieceio.verify_piece(index)
        with self.udl_lock:
            self.udl[2] = self.piecio.data_left()
        if all(self.pieceio.bitfield):
            self.reannounce(3)
        return success
        

client = BitTorrentClient(metainfo.TEST_TORRENT)
p = client.peer_manager
while not p.connections and not bleh:
    import time
    time.sleep(1)
if not bleh:
    c = p.connections[0]
    q = c.queue
