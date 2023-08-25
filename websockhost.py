from gtfs_compiler import compileGTFS
from websockets.server import serve
from websockets.client import connect
import asyncio
import aiofiles
import time
import json
import websockets
from hashlib import sha256

UPSTREAM_CLIENTS = set()

LAST_POS_UPDATES = {}
LAST_DATA_HASH = ''
LAST_GTFS = bytearray()

async def handleUpstreamConnection(websocket, serversock):
    global UPSTREAM_CLIENTS, LAST_GTFS, LAST_DATA_HASH
    async for message in websocket:
        print(UPSTREAM_CLIENTS)
        try:
            message_payload = json.loads(message)
            match message_payload['msg_type']:
                case 'ds':
                    print('omgf')
                    folder = f"gtfs{str(int(time.time()))}"

                    h = sha256()
                    h.update(message.encode('utf8'))
                    gtfs_hash = h.hexdigest()
                    if gtfs_hash != LAST_DATA_HASH:
                        hash_updated = True
                    else: hash_updated = False

                    print(hash_updated)

                    if hash_updated:
                        compileGTFS(message_payload, folder)
                        async with aiofiles.open(f'./{folder}.zip', 'rb') as f:
                            gtfs = bytearray(await f.read())
                        LAST_GTFS = gtfs
                        LAST_DATA_HASH = gtfs_hash

                        disabled_clients = set()
                        for client in UPSTREAM_CLIENTS:
                            try:
                                # await client.send(json.dumps({'msg_type': 'gtfs_updated'}))
                                await client.send(gtfs)
                            except websockets.exceptions.ConnectionClosed:
                                disabled_clients.add(client)
                        #     try:
                        #         await asyncio.wait_for(client.send(gtfs), timeout=1.0)
                        #     except asyncio.TimeoutError:
                        #         await client.close()
                        #         disabled_clients.add(client)
                        UPSTREAM_CLIENTS = UPSTREAM_CLIENTS.difference(disabled_clients)

                case 'tm':
                    print('tm_sent')
                    for update in message_payload['data']:
                        LAST_POS_UPDATES[update['id']] = update
                    # TODO: get info for gtfs rt
                    disabled_clients = set()
                    for client in UPSTREAM_CLIENTS:
                        try:
                            await client.send(message)
                        except websockets.exceptions.ConnectionClosed:
                            disabled_clients.add(client)
                    #     try:
                    #         await asyncio.wait_for(client.send(message), timeout=1.0)
                    #     except asyncio.TimeoutError:
                    #         await client.close()
                    #         disabled_clients.add(client)
                    UPSTREAM_CLIENTS = UPSTREAM_CLIENTS.difference(disabled_clients)
        except Exception:
            continue

async def handleConnection(websocket):
    print('connection established')
    UPSTREAM_CLIENTS.add(websocket)
    while True:
        try:
            message = await websocket.recv()
            if message.strip() == 'reqctm':
                collected_updates = [x for x in LAST_POS_UPDATES.values()]
                payload = {
                    'msg_type': 'ctm',
                    'data': collected_updates
                }
                await websocket.send(json.dumps(payload))
            elif message.strip() == 'reqgtfs':
                await websocket.send(LAST_GTFS)
        except Exception:
            UPSTREAM_CLIENTS.remove(websocket)

async def main():
    async with serve(handleConnection, "localhost", 8192) as websock_server:
        async with connect("ws://localhost:8989", max_size=2**40) as websock_client:
            await handleUpstreamConnection(websock_client, websock_server)

asyncio.run(main())