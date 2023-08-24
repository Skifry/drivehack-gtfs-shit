import asyncio
from websockets.server import serve

import json

DS_CLIENTS = set()
TM_CLIENTS = set()

ctime = 1692688349
ds_compl = ()

data = json.load(open('tel.json'))

async def handler(websocket):
    message = await websocket.recv()
    if message.strip() not in ['ds', 'tm']:
        await websocket.send('not supported')

    match message.strip():
        case 'ds':
            DS_CLIENTS.add(websocket)
        case 'tm':
            TM_CLIENTS.add(websocket)

    await asyncio.Future()

async def emulator():
    global ctime, ds_compl
    while True:
        if ctime % 60 == 0:
            ds_docks = json.load(open('docks.json'))
            ds_schedule = json.load(open('schedule.json'))
            ds_ships = json.load(open('ships.json'))
            
            ds_compl = {'docks': ds_docks, 'schedule': ds_schedule, 'ships': ds_ships}

            for client in DS_CLIENTS:
                await client.send(json.dumps(ds_compl))
        
        tm_to_send = []
        for ent in data:
            if ent['ts'] == ctime:
                tm_to_send.append(ent)
                data.remove(ent)
        
        tm_data = json.dumps(tm_to_send)
        if TM_CLIENTS:
            if tm_to_send:
                for client in TM_CLIENTS:
                    await client.send(tm_data)
                    
        ctime += 1
        print(ctime)
        await asyncio.sleep(1)
        

async def main():
    async with serve(handler, 'localhost', 8989):
        await emulator()

asyncio.run(main())