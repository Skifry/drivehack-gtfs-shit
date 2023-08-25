from gtfs_compiler import compileGTFS
from websockets.server import serve
from websockets.client import connect
import asyncio
import aiofiles
import time
import json

UPSTREAM_CLIENTS = set()

LAST_POS_UPDATES = {}

async def handleUpstreamConnection(websocket, serversock):
    async for message in websocket:
        try:
            print(message)
            message_payload = json.loads(message)
            match message_payload['msg_type']:
                case 'ds':
                    print('omgf')
                    folder = f"gtfs{str(int(time.time()))}"
                    compileGTFS(message_payload, folder)
                    async with aiofiles.open(f'./{folder}.zip', 'rb') as f:
                        gtfs = bytearray(await f.read())
                        disabled_clients = set()
                        for client in UPSTREAM_CLIENTS:
                            try:
                                await asyncio.wait_for(client.send(gtfs), timeout=1.0)
                            except asyncio.TimeoutError:
                                await client.close()
                                disabled_clients.add(client)
                        UPSTREAM_CLIENTS = UPSTREAM_CLIENTS.difference(disabled_clients)

                case 'tm':
                    for update in message_payload['data']:
                        LAST_POS_UPDATES[update['id']] = update
                    # TODO: get info for gtfs rt
                    disabled_clients = set()
                    for client in UPSTREAM_CLIENTS:
                        try:
                            await asyncio.wait_for(client.send(message), timeout=1.0)
                        except asyncio.TimeoutError:
                            await client.close()
                            disabled_clients.add(client)
                    UPSTREAM_CLIENTS = UPSTREAM_CLIENTS.difference(disabled_clients)
        except Exception:
            continue

async def handleConnection(websocket):
    UPSTREAM_CLIENTS.add(websocket)
    collected_updates = [x for x in LAST_POS_UPDATES.values()]
    payload = {
        'msg_type': 'ctm',
        'data': collected_updates
    }
    await websocket.send(json.dumps(payload))
    await asyncio.Future()

async def main():
    async with serve(handleConnection, "localhost", 8192) as websock_server:
        async with connect("ws://localhost:8989", max_size=2**40) as websock_client:
            await handleUpstreamConnection(websock_client, websock_server)

asyncio.run(main())