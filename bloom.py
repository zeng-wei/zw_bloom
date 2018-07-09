import mmh3
import redis
from redlock import Redlock

BIT_SIZE = 2**32


class Bloom:

    def __init__(self, key, host='127.0.0.1', port=6379, db=0):
        self.key = key  # redis-bitmap的key
        self.redis_cli = redis.StrictRedis(host=host, port=port, db=db, charset='utf-8')
        self.red_lock = Redlock([{"host": host, "port": port, "db": db}, ])

    def add(self, value):
        value_unique_lock = self.red_lock.lock(value, 60*1000)
        if value_unique_lock is False:
            return False
        point_list = self.get_positions(value)
        for b in point_list:
            exists = self.redis_cli.getbit(self.key, b)
            if not exists:
                break
        else:
            return False
        for b in point_list:
            self.redis_cli.setbit(self.key, b, 1)
        return True

    def contains(self, value):
        point_list = self.get_positions(value)
        for b in point_list:
            exists = self.redis_cli.getbit(self.key, b)
            if not exists:
                return False
        return True

    @staticmethod
    def get_positions(value):
        point0 = mmh3.hash(value, 40, signed=False)
        point1 = mmh3.hash(value, 41, signed=False)
        point2 = mmh3.hash(value, 42, signed=False)
        point3 = mmh3.hash(value, 43, signed=False)
        point4 = mmh3.hash(value, 44, signed=False)
        point5 = mmh3.hash(value, 45, signed=False)
        point6 = mmh3.hash(value, 46, signed=False)
        point7 = mmh3.hash(value, 47, signed=False)

        return [point0, point1, point2, point3, point4, point5, point6, point7]


if __name__ == "__main__":
    bloom = Bloom("bloom")
    count = 0
    for i in range(10000, 20000):
        count += bloom.add(str(i))
    print(count)