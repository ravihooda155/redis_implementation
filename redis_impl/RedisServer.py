import argparse
import sys
import console
from rest_api import RestServer
from CmdExecution import *

log = logging.getLogger(__name__)

class RedisServer(object):

    def __init__(self, cmd=True, rest=True, port=None):
        print 'Starting MiniRedis Server...'
        super(RedisServer, self).__init__()
        self.cmdexecution = CmdExecution()
        self.cmd = console.Console(self.cmdexecution)
        self.restServer = None
        if rest:
            print 'Redis API REST STARTING ...'
            self.restServer = RestServer(cmd=self.cmd, port=port)
        if cmd:
            if self.restServer:
                th = threading.Thread(target=self.restServer.start_server)
                th.daemon = True
                th.start()
            try:
                self.cmd.cmdloop('Redis Console...')
            except (SystemExit, KeyboardInterrupt):
                if self.restServer:
                    self.restServer.stop_server()
                    self.restServer.miniredis = None
                print 'Redis Console Closed'
                sys.exc_clear()
        else:
            if self.restServer:
                self.restServer.start_server()


if __name__ == '__main__':

    print '                                                        '
    print '--------------------------------------------------------'
    print '-> Command Line'
    print '-> HTTP Client http://localhost:8080?cmd={command}'
    print '--------------------------------------------------------'
    print '                                                        '
    parser = argparse.ArgumentParser()
    parser.set_defaults(cmd=True, rest=True,p=8080)
    args = parser.parse_args()
    server = RedisServer(cmd=args.cmd, rest=args.rest, port=args.p)
