import asyncio
from motor import motor_asyncio
from pymongo.errors import AutoReconnect, ConnectionFailure
import logging
from urllib.parse import urlparse
# import pprint
log = logging.getLogger(__name__)


class AsyncMongo():
    DEFAULT_PORT = 27017

    def __init__(self, uri=None, loop=None, collection='links'):

        _uri = uri or 'mongodb://localhost:27017/mean'
        _url = urlparse(_uri)
        _host = _url.hostname or 'localhost'
        _port = _url.port or '27017'
        # _login = _url.username
        # _password = _url.password
        _db_name = _url.path[1:]

        log.debug("{}:{}/{}".format(_host, _port, _db_name))
        self.uri = _uri
        self.db_name = _db_name or 'mean'
        self.loop = loop or asyncio.get_event_loop()
        self.connected = False
        self._conn = None
        self.collection = collection

    async def do_find(self, collection=None, filter=None):
        while self.connected is not True:
            await asyncio.sleep(1.0, loop=self.loop)
        _collection = collection or self.collection
        c = self.db[_collection]
        docs = []
        async for doc in c.find(filter):
            docs.append(doc)
        log.debug("Collection [{}]: {}".format(_collection, docs))
        return docs

    async def do_insert(self, collection, data):
        while self.connected is not True:
            await asyncio.sleep(1.0, loop=self.loop)
        _collection = collection
        c = self.db[_collection]
        c.insert_one(data)

    async def do_update(self, collection, conditions, content):
        while self.connected is not True:
            await asyncio.sleep(1.0, loop=self.loop)
        _collection = collection
        c = self.db[_collection]
        c.update(conditions, content)


    #############################################################
    async def _connect(self):
        self._conn = motor_asyncio.AsyncIOMotorClient(
            self.uri,
            io_loop=self.loop
        )
        try:
            self.connected = await self.wait_db()
        except AutoReconnect as e:
            log.error("Couldn't connect to db %s", self.uri)
            self.connected = await self.wait_db()
        if self.connected:
            self.db = self._conn[self.db_name]
            log.info('Connection Successfully.')

    async def _disconnect(self):
        if self._conn is not None:
            self._conn = None

    async def ping(self):
        try:
            await self._conn.admin.command({'ping': 1})
            return True
        except ConnectionFailure:
            log.error('Connection Failure.')
            return False

    async def wait_db(self):
        pong = False
        while not pong:
            pong = await self.ping()
            if not pong:
                log.warning('%s is unavailable. Waiting.',
                            self.uri)
                await asyncio.sleep(1.0, loop=self.loop)
        return True

    async def reconnector(self):
        while True:
            if self.connected is True:
                await asyncio.sleep(10.0, loop=self.loop)
            else:
                await self._disconnect()
            self.connected = False
            await self._connect()


def main(debug=True):
    # configure log
    log = logging.getLogger("")
    formatter = logging.Formatter("%(asctime)s %(levelname)s " +
                                  "[%(module)s] %(message)s")
    # log the things
    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    # ch.setLevel(logging.DEBUG)
    # ch.setLevel(logging.ERROR)
    # ch.setLevel(logging.CRITICAL)
    if debug:
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    global loop
    loop = asyncio.get_event_loop()
    loop.set_debug(0)

    db = AsyncMongo(uri='mongodb://172.20.95.39:27017/mean')
    db_task = loop.create_task(db.reconnector())
    collection = 'alarms'
    data = {
        "system" : 22,
        "alarmType" : "COMM FAIL",
        "time_stamp" : "2018-05-30 14:44:40",
        "offset" : 0,
        "detail" : "0",
        "actions" : [ ],
        "counter" : 1,
        "description" : "设备通讯失败",
        "notes" : "",
        "level" : "3",
        "selectedUser" : [ ],
        "latlng" : [ 25.122288, 102.941537 ],
        "status" : "OCCURRED",
        "createdTime" : [ "2018-05-30 14:44:44" ],
        "name" : "PMF_22_2"}
    loop.run_until_complete(db.do_insert(collection, data))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        db_task.cancel()
        loop.run_until_complete(db_task)
    finally:
        loop.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        exit(1)
