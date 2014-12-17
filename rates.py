import threading

class TransferRates:

    def __init__(self, win_size):
        self.win_size = win_size
        self.count = [0, 0]
        self.window = [(0, 0)]*self.win_size
        self.lock = threading.Lock()

    def up(self):
        return self.window[-1][0]

    def down(self):
        return self.window[-1][1]

    def up_avg(self, win_size=None):
        return self._avg(0, win_size)

    def down_avg(self, win_size=None):
        return self._avg(1, win_size)

    def _avg(self, i, win_size=None):
        if win_size is None:
            win_size = self.win_size
        elif win_size > self.win_size or win_size < 0:
            raise ValueError, 'invalid win_size'
        with self.lock:
            window = [rates[i] for rates in self.window[-win_size:]]
            return float(sum(window)) / win_size

    def update(self, up, down):
        with self.lock:
            self.count[0] += up
            self.count[1] += down

    def tick(self):
        with self.lock:
            self.window.pop(0)
            self.window.append(self.count)
            self.count = [0, 0]
