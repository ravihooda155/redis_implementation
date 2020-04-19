import logging
import unittest
from time import sleep

from redis_impl.CmdExecution import CmdExecution
from redis_impl.structures import *

log_format = "%(asctime)s %(funcName)20s %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.DEBUG, format=log_format)
log = logging.getLogger(__name__)


def multithread_worker(cmdexecution, key, th):
    log.debug('thread {})  {}'.format(th, cmdexecution.incr(key)))
    return


class TestRedis(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.cmdexecution = CmdExecution()

    def test_get_empty(self):
        key = 'empty'
        resp = self.cmdexecution.get(key)
        self.assertEquals(resp, EMPTY_VALUE)

    def test_set(self):
        key = 'key'
        value = 10
        resp = self.cmdexecution.set(key, value)
        self.assertEquals(resp, OK_VALUE)
        resp = self.cmdexecution.get(key)
        self.assertEquals(resp, value)

    def test_set_seconds(self):
        key = 'key_sec'
        value = 5
        resp = self.cmdexecution.set(key, value, 3)
        self.assertEquals(resp, OK_VALUE)
        resp = self.cmdexecution.get(key)
        self.assertEquals(resp, value)
        sleep(4)
        resp = self.cmdexecution.get(key)
        self.assertEquals(resp, EMPTY_VALUE)

    def test_incr_multithreading(self):
        key = 'threads'
        threads = []
        for i in range(50):
            t = threading.Thread(name='thread_' + str(i + 1), target=multithread_worker,
                                 args=(self.cmdexecution, key, i + 1))
            threads.append(t)
        for i in range(50):
            threads[i].start()
            if (i % 5) == 0:
                log.debug('main thread) i:{}  {}'.format(i, self.cmdexecution.incr(key)))
        for i in range(50):
            threads[i].join()
        log.debug('main thread) final  {}'.format(self.cmdexecution.incr(key)))
        self.assertEquals(int(self.cmdexecution.get(key)), 61)

    def test_del(self):
        key = 'del'
        value = 5
        resp = self.cmdexecution.set(key, value)
        self.assertEquals(resp, OK_VALUE)
        resp = self.cmdexecution.get(key)
        self.assertEquals(resp, value)
        resp = self.cmdexecution.delete(key)
        self.assertEquals(resp, 1)
        resp = self.cmdexecution.get(key)
        self.assertEquals(resp, EMPTY_VALUE)

    def test_z_functions(self):
        key = 'zkey'
        resp = self.cmdexecution.zadd(key, '10', 'a')
        self.assertEquals(resp, 1)
        resp = self.cmdexecution.zadd(key, '30', 'c')
        self.assertEquals(resp, 1)
        resp = self.cmdexecution.zadd(key, '20', 'b')
        self.assertEquals(resp, 1)
        resp = self.cmdexecution.zcard(key)
        self.assertEquals(resp, 3)
        resp = self.cmdexecution.zrange(key, 0, -1,None)
        self.assertEquals(resp, ['a', 'b', 'c'])
        resp = self.cmdexecution.zrank(key, 'a')
        self.assertEquals(resp, 0)
        resp = self.cmdexecution.zrevrank(key, 'a')
        self.assertEquals(resp, 2)
        resp = self.cmdexecution.zadd(key, '50', 'a')
        self.assertEquals(resp, 0)
        resp = self.cmdexecution.zrank(key, 'a')
        self.assertEquals(resp, 2)
        resp = self.cmdexecution.zrange(key, 0, -1,None)
        self.assertEquals(resp, ['b', 'c', 'a'])
        resp = self.cmdexecution.zrevrange(key, 0, -1,None)
        self.assertEquals(resp, ['a', 'c', 'b'])
        resp = self.cmdexecution.zrank(key, 'x')
        self.assertEquals(resp, EMPTY_VALUE)
        resp = self.cmdexecution.zrange('x', 0, -1,None)
        self.assertEquals(resp, EMPTY_LIST)

    def test_handle_errors(self):
        key = 'err'
        resp = self.cmdexecution.get(None)
        self.assertEquals(resp, ERROR_ARGUMENT)
        resp = self.cmdexecution.set(key, 1, 3.3)
        self.assertEquals(resp, ERROR_INTEGER)
        resp = self.cmdexecution.set(key, 1, 0)
        self.assertEquals(resp, ERROR_EXPIRE)
        resp = self.cmdexecution.set(key, 1)
        self.assertEquals(resp, OK_VALUE)
        resp = self.cmdexecution.zadd(key, '10', 'a')
        self.assertEquals(resp, ERROR_WRONG_TYPE)
        resp = self.cmdexecution.delete(key)
        self.assertEquals(resp, 1)
        resp = self.cmdexecution.zadd(key, 'x', 'a')
        self.assertEquals(resp, ERROR_FLOAT)


if __name__ == "__main__":
    unittest.main()
