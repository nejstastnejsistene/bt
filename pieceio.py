import hashlib
import io
import threading
import os

import bitfield


def PieceIO(info, dl_dir):
    if 'length' in info:
        self = SingleFilePieceIO(info, dl_dir)
    else:
        self =  MultiFilePieceIO(info, dl_dir)
    self.create_bitfield()
    return self


class BasePieceIO:

    def __init__(self, info, dl_dir):
        self.info = info
        self.dl_dir = dl_dir
        self.locks = {}

    def read_piece(self, index, begin, length, create=False):
        buf = io.BytesIO()
        self.handle_piece(index, begin, length, buf, True, create)
        buf.seek(0)
        return buf.read()
        
    def write_piece(self, index, begin, data, create=False):
        buf = io.BytesIO(data)
        return self.handle_piece(index, begin, len(data), buf, False, create)

    def handle_piece(self, index, begin, length, buf, read, create):
        end = begin + length
        while begin < end:
            try:
                path, offset, flen = self.position(index, begin)
            except EndReachedException:
                break
            size = min(flen - offset, end - begin)
            dirs = os.path.dirname(path)
            if not os.path.exists(dirs):
                if not create:
                    raise FileMissingError, path
                os.makedirs(dirs)
            with self.locks[path]:
                if not os.path.exists(path):
                    if not create:
                        raise FileMissingError, path
                    fd = open(path, 'w+b')
                    fd.seek(flen - 1)
                    fd.write(chr(0))
                else:
                    fd = open(path, 'r+b')
                fd.seek(offset)
                if read:
                    data = fd.read(size)
                    buf.write(data)
                else:
                    data = buf.read(size)
                    fd.write(data)
                fd.close()
            begin += size
        return length

    def create_bitfield(self):
        self.bitfield = bitfield.Bitfield(self.info.num_pieces)
        for i in range(self.info.num_pieces):
            self.verify_piece(i, False)

    def verify_piece(self, index, clear=True):
        if self.verify_hash(index):
            self.bitfield[index] = 1
            return True
        elif clear:
            self.clear_piece(index)
        return False

    def verify_hash(self, index):
        sha1 = hashlib.sha1()
        data = self.read_piece(index, 0, self.info.piece_size, True)
        sha1.update(data)
        return sha1.digest() == self.info.pieces[index]

    def clear_piece(self, index):
        data = chr(0)*self.info.piece_size
        self.write_piece(index, 0, data)

    def data_left(self):
        left = self.total_size
        for bit in self.bitfield[:-1]:
            if bit:
                left -= self.info.piece_size
        if self.bitfield[-1]:
            left -= self.total_size % self.info.piece_size
        return left
    

class SingleFilePieceIO(BasePieceIO):

    def __init__(self, info, dl_dir):
        BasePieceIO.__init__(self, info, dl_dir)
        self.path = self.dl_dir + [self.info['name']]
        self.path = os.path.join(*self.path)
        self.locks[self.path] = threading.Lock()
        self.total_size = self.info['length']
    
    def position(self, index, begin):
        offset = index * self.info.piece_size + begin
        if offset == self.info['length']:
            raise EndReachedException
        return self.path, offset, self.info['length']


class MultiFilePieceIO(BasePieceIO):

    def __init__(self, info, dl_dir):
        BasePieceIO.__init__(self, info, dl_dir)
        self.dl_dir += [self.info['name']]
        self._mapping = [(0, 0)]
        self.total_size = 0
        index = 0
        for i in range(len(self.info['files'])):
            f = self.info['files'][i]
            index += f['length']
            self.total_size += f['length']
            while index >= self.info.piece_size:
                offset = self.info.piece_size - (index - f['length'])
                self._mapping.append((i, offset))
                index -= self.info.piece_size

    def mapping(self, index, begin):
        files = self.info['files']
        i, offset = self._mapping[index]
        offset += begin
        try:
            while offset >= files[i]['length']:
                offset -= files[i]['length']
                i += 1
        except IndexError:
            raise EndReachedException
        return i, offset

    def position(self, index, begin):
        i, offset = self.mapping(index, begin)
        f = self.info['files'][i]
        path = self.dl_dir + f['path']
        path = os.path.join(*path)
        if not path in self.locks:
            self.locks[path] = threading.Lock()
        return path, offset, f['length']


class PieceIOError(Exception):
    pass

class FileMissingError(PieceIOError):
    pass

class EndReachedException(PieceIOError):
    pass

