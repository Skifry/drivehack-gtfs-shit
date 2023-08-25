from gtfs_compiler import compileGTFS
from websockets.server import serve
from websockets.client import connect
import websockets
import asyncio
import aiofiles

import json

UPSTREAM_CLIENTS = set()

async def handleUpstreamConnection(websocket, serversock):
    async for message in websocket:
        message_payload = json.loads(message)
        match message_payload['msg_type']:
            case 'ds':
                compileGTFS(message_payload)
                async with aiofiles.open('./gtfs479111.zip', 'rb') as f:
                    gtfs = bytearray(await f.read())
                    for client in UPSTREAM_CLIENTS:
                        client.send(gtfs)

            case 'tm':
                # TODO: get info for gtfs rt
                pass

async def handleConnection(websocket):
    UPSTREAM_CLIENTS.add(websocket)
    await asyncio.Future()

async def main():
    async with serve(handleConnection, "ws://localhost:8192") as websock_server:
        async with connect("ws://localhost:8989") as websock_client:
            await handleUpstreamConnection(websock_client, websock_server)

asyncio.run(main())