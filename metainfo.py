import hashlib
import bencode

TEST_TORRENT = 'ubuntu-10.10-desktop-i386.iso.torrent'
#TEST_TORRENT = 'Heart.torrent'

class MetaInfo(dict):

    def __init__(self, metainfo_file):
        with open(metainfo_file, 'rb') as f:
            data = f.read()
        metainfo = bencode.bdecode(data)
        self.info = Info(metainfo['info'])
        self['info'] = self.info
        for key, value in metainfo.items():
            if key != 'info':
                self[key] = value
        info_start = data.index('4:info') + 6
        info_end = bencode._bdecode(data, info_start)[1]
        sha1 = hashlib.sha1()
        sha1.update(data[info_start:info_end])
        self.info_hash = sha1.digest()
        self.info_hash_hex = sha1.hexdigest()

class Info(dict):

    def __init__(self, info):
        self.pieces = []
        for i in range(0, len(info['pieces']), 20):
            self.pieces.append(info['pieces'][i:i+20])
        self.num_pieces = len(self.pieces)
        self.piece_size = info['piece length']
        for key, value in info.items():
            self[key] = value

metainfo = MetaInfo(TEST_TORRENT)
