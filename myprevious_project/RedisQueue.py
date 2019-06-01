import redis,os
import time

class RedisQueue(object):
    """Simple Queue with Redis Backend"""
    def __init__(self, name, namespace='queue'):
       self.__db= redis.Redis(host='localhost', port=6379, db=0)
       self.key = '%s:%s' %(namespace, name)

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def put(self, item):
        """Put item into the queue."""
        self.__db.rpush(self.key, item)

    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue. 

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            item = self.__db.blpop(self.key, timeout=timeout)
        else:
            item = self.__db.lpop(self.key)

        if item:
            item = item[1]
        return item

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)

		
if __name__ == '__main__':
	
	q = RedisQueue('test')
	q.put('hello world')
	q.put('hello Jegan')
	q.put('hello Sathish')
	q.put('hello Rajesh')
	q.put('hello Hari')

	# time.sleep(2)
	while q.qsize() >= 1:
		print "Getting Queue",q.get()
		os.system("start python find_position.py")