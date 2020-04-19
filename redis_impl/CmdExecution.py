import logging

from structures import *

log = logging.getLogger(__name__)


def set_to_list(dic,score=None):
    if score is None:
	    return [str(x) for _, x in sorted(zip(dic.values(), dic.keys()))]
    else:
        res=[]
        for y,x in sorted(zip(dic.values(),dic.keys())):
            res.append(str(x))
            res.append(str(int(y)))
        return (res)
            
def set_to_list___rev(dic,score=None):
    if score is None:
	    return [str(x) for _, x in sorted(zip(dic.values(), dic.keys()),reverse=True)]
    else:
        res=[]
        for y,x in sorted(zip(dic.values(),dic.keys()),reverse=True):
            res.append(str(x))
            res.append(str(int(y)))
        return (res)
            

def set_to_list_rev(dic):
    return [str(x) for _, x in sorted(zip(dic.values(), dic.keys()),reverse=True)]

class CmdExecution(object):

    def __init__(self):
        self.db = ThreadSafeDict()
        self.tasks = ThreadSafeDict()

    def delete_timer(self, key):
        print("deleting key {}".format(key))
        self.delete(key)

    def set(self, key, value, seconds=None):
        if key is None or value is None:
            return ERROR_ARGUMENT
        if seconds is not None:
            try:
                seconds = int(str(seconds))
            except (KeyError, TypeError, ValueError):
                return ERROR_INTEGER
            if seconds < 1:
                return ERROR_EXPIRE
        with self.db as db:
            with self.tasks as tasks:
                if key in tasks:
                    tasks[key].cancel()
                    del tasks[key]
                db[key] = value
                if seconds is not None:
                    new_task = threading.Timer(seconds, self.delete, args=[key])
                    new_task.start()
                    tasks[key] = new_task
        return OK_VALUE

    def get(self, key):
        if key is None:
            return ERROR_ARGUMENT
        with self.db as db:
            value = db.get(key)
            if isinstance(value, dict):
                return ERROR_WRONG_TYPE
            if value is not None:
                return value
            else:
                return EMPTY_VALUE

    def delete(self, key):
        if key is None:
            return ERROR_ARGUMENT
        with self.db as db:
            if key not in db:
                return 0
            with self.tasks as task:
                if key in task:
                    task[key].cancel()
                    del task[key]
                del db[key]
        return 1

    def expire(self,key,seconds=None):
        if key is None:
            return 0
        if seconds is None:
            return ERROR_ARGUMENT
        if seconds is not None:
            try:
                seconds = int(str(seconds))
            except (KeyError, TypeError, ValueError):
                return ERROR_INTEGER
            if seconds < 1:
                return ERROR_EXPIRE
        with self.db as db:
            with self.tasks as tasks:
                value=db.get(key)
                if key in tasks:
                    tasks[key].cancel()
                    del tasks[key]
                db[key] = value
                if seconds is not None:
                    new_task = threading.Timer(seconds, self.delete, args=[key])
                    new_task.start()
                    tasks[key] = new_task
        return OK_VALUE


    def db_size(self):
        with self.db as db:
            return len(db)

    def incr(self, key):
        if key is None:
            return ERROR_ARGUMENT
        with self.db as db:
            val = db.get(key)
            if val is None:
                val = 0
            try:
                val = int(str(val)) + 1
                db[key] = str(val)
                return val
            except (KeyError, TypeError, ValueError):
                return ERROR_INTEGER

    def zadd(self, key, score, member):
        try:
            score = float(str(score))
        except (KeyError, TypeError, ValueError):
            return ERROR_FLOAT
        if key is None or score is None or member is None:
            return ERROR_ARGUMENT
        with self.db as db:
            z = db.get(key)
            if z is None:
                z = {}
            elif not isinstance(z, dict):
                return ERROR_WRONG_TYPE
            if member in z:
                val = 0
            else:
                val = 1
            z[member] = score
            db[key] = z
            return val

    def zcard(self, key):
        if key is None:
            return ERROR_ARGUMENT
        with self.db as db:
            z = db.get(key)
            if z is None:
                return 0
            elif not isinstance(z, dict):
                return ERROR_WRONG_TYPE
            return len(z)

    def zrank(self, key, member):
        if key is None or member is None:
            return ERROR_ARGUMENT
        with self.db as db:
            z = db.get(key)
            if z is None:
                return EMPTY_VALUE
            elif not isinstance(z, dict):
                return ERROR_WRONG_TYPE
            if member in z:
                return set_to_list(z).index(member)
            else:
                return EMPTY_VALUE


    def zrevrank(self, key, member):
        if key is None or member is None:
            return ERROR_ARGUMENT
        with self.db as db:
            z = db.get(key)
            if z is None:
                return EMPTY_VALUE
            elif not isinstance(z, dict):
                return ERROR_WRONG_TYPE
            if member in z:
                return set_to_list_rev(z).index(member)
            else:
                return EMPTY_VALUE

    def zrange(self, key, start, stop,withscore):
        try:
            start = int(str(start))
            stop = int(str(stop))
        except (KeyError, TypeError, ValueError):
            return ERROR_INTEGER
        if key is None:
            return ERROR_ARGUMENT
        with self.db as db:
            z = db.get(key)
            if z is None:
                return EMPTY_LIST
            elif not isinstance(z, dict):
                return ERROR_WRONG_TYPE
            if withscore is None:
                zlist = set_to_list(z,None)
                size = len(zlist)
            else:
                zlist=set_to_list(z,1)
                #stop=stop*2+1
                size = len(zlist)
            
            if start < 0:
                start = size + start
            if stop < 0:
                stop = size + stop
            if withscore is not None:
                stop=stop*2+1

            result = zlist[start:stop + 1]
            if not result:
                return EMPTY_LIST
            else:
                return result

    def zrevrange(self, key, start, stop,withscore):
        try:
            start = int(str(start))
            stop = int(str(stop))
        except (KeyError, TypeError, ValueError):
            return ERROR_INTEGER
        if key is None:
            return ERROR_ARGUMENT
        with self.db as db:
            z = db.get(key)
            if z is None:
                return EMPTY_LIST
            elif not isinstance(z, dict):
                return ERROR_WRONG_TYPE
            if withscore is None:
                zlist = set_to_list___rev(z,None)
                size = len(zlist)
            else:
                zlist=set_to_list___rev(z,1)
                #stop=stop*2+1
                size = len(zlist)
            
            if start < 0:
                start = size + start
            if stop < 0:
                stop = size + stop
            if withscore is not None:
                stop=stop*2+1

            result = zlist[start:stop + 1]
            if not result:
                return EMPTY_LIST
            else:
                return result

