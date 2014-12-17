
class Bitfield:

    def __init__(self, size, bitstring=''):
        self.size = size
        self.num_bytes = self.size / 8
        if self.size % 8:
            self.num_bytes += 1
        self.num_true = 0
        self.bits = [0] * self.size
        if bitstring:
            self.unpack(bitstring)

    def pack(self):
        bitstring = ''
        for i in range(self.num_bytes):
            bits = self.bits[i*8:i*8+8]
            byte = 0
            for i in range(len(bits)):
                byte |= bits[i] << (7 - i)
            bitstring += chr(byte)
        return bitstring

    def unpack(self, bitstring):
        if len(bitstring) != self.num_bytes:
            raise ValueError, 'incorrect bitstring length'
        empty_bits = len(bitstring) * 8 - self.size
        for i in range(empty_bits):
            if ord(bitstring[-1]) & (1 << i):
                raise ValueError, 'unused bits are used'
        self.num_true = 0
        for i in range(self.size):
            bit = ord(bitstring[i/8]) >> (7 - i % 8) & 1
            if bit:
                self.num_true += 1
                self.bits[i] = bit
                
    def __getitem__(self, index):
        return self.bits[index]

    def __setitem__(self, index, flag=1):
        if flag != 0 and flag != 1:
            raise ValueError, 'flag must be 0 or 1'
        self.bits[index] = flag
        self.num_true += flag

    def __len__(self):
        return self.size

    def __cmp__(self, other):
        return cmp(self.num_true, other.num_true)

    def clone(self):
        bf = Bitfield(self.size)
        for i in range(self.size):
            bf.bits[i] = self.bits[i]
        return bf
