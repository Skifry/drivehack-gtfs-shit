from collections import defaultdict
import json
import time
import os
import shutil
import datetime

class hashabledict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))
    
def fileWriter(head, rows, filename):
    lines = []
    lines.append(','.join(head) + '\n')

    for row in rows:
        for idx, item in enumerate(row):
            if type(item) == None:
                row[idx] = ''
            elif type(item) == bool:
                row[idx] = int(item)
            row[idx] = str(row[idx])
        lines.append(','.join(row) + '\n')
        
    
    with open(filename, 'w') as f:
        f.writelines(lines)

def compileGTFS(ds_compl):
    folder = f"gtfs{str(int(time.time()))}"
    os.makedirs(folder)

    # static generation
    shutil.copyfile('./static/agency.txt', f'./{folder}/agency.txt')

    # calendar generation
    calendar_head = ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date']

    # today_wk = datetime.datetime.today().weekday()
    # today_datea = datetime.date.today().timetuple().tm_yday
    # today_frmt = datetime.date.today().strftime('%Y%m%d')

    today_wk = datetime.datetime.fromtimestamp(1692824400).weekday()
    today_datea = datetime.date.fromtimestamp(1692824400).timetuple().tm_yday
    today_frmt = datetime.date.fromtimestamp(1692824400).strftime('%Y%m%d')

    SERVICE_ID = f'c{today_datea}'
    calendar_row = [SERVICE_ID, False, False, False, False, False, False, False, today_frmt, today_frmt]
    calendar_row[1 + today_wk] = True
    f_calendar = [calendar_head, calendar_row]


    # stops generation
    stops_head = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'location_type', 'parent_station', 'platform_code']
    stops_rows = []

    # parsing schedule for gathering platforms
    coded_platforms = {}
    platforms = defaultdict(lambda: [])
    for schedule in ds_compl['schedule']:
        berth_letter = schedule['changed_schedule']['berth_letter'] or schedule['position']['berth_letter']
        platform_id = f"{str(schedule['dock']['id'])}_{berth_letter}"
        platform_name = f'_{berth_letter}'
        platform = {'id': platform_id, 'name': platform_name, 'code': berth_letter}
        coded_platforms[platform_id] = platform
    for k, v in coded_platforms.items():
        dock_id = k.split('_')[0]
        platforms[dock_id].append(v)

    # stations from docks
    for dock in ds_compl['docks']:
        stop_id = dock['id']
        stop_name = dock['name_mark']
        stop_lat = dock['geopoint'][1]
        stop_lon = dock['geopoint'][0]
        location_type = 1
        parent_station = ''
        platform_code = ''
        stop_row = [stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station, platform_code]
        stops_rows.append(stop_row)

        for platform in platforms[str(dock['id'])]:
            parent_station = dock['id']
            stop_id = platform['id']
            stop_name = dock['name_mark'] + platform['name']
            location_type = 0
            platform_code = platform['code']
            stop_row = [stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station, platform_code]
            stops_rows.append(stop_row)
        

    # new filtering schedule
    n_schedule = []
    for schedule in ds_compl['schedule']:
        if schedule['changed_schedule']['type'] == 2:
            continue
        if schedule['timetable']['startdate'] != 1692824400:
            continue
        n_schedule.append(schedule)
    ds_compl['schedule'] = n_schedule


    # routes generation
    routes_counter = 1
    routes = []
    wiroutes = set()
    for schedule in ds_compl['schedule']:
        route_id = schedule['route']['id']
        route_abbrev = schedule['route']['abbrev']
        route_name = schedule['route']['nameroute']
        route = {'id': routes_counter, 'abbrev': route_abbrev, 'name': route_name}
        wiroute = hashabledict({'abbrev': route_abbrev, 'name': route_name})
        if wiroute not in wiroutes:
            routes_counter += 1
            routes.append(route)
            wiroutes.add(wiroute)

    routes_head = ['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type', 'route_url']
    routes_rows = []
    r_agency_id = 'MST'
    r_route_type = 4
    r_route_url = 'https://t.mos.ru/reka/routes'
    for route_unf in routes:
        route = [
            route_unf['id'],
            r_agency_id,
            route_unf['abbrev'],
            route_unf['name'],
            r_route_type,
            r_route_url
        ]
        routes_rows.append(route)


    # generating trips
    trips_head = ['route_id', 'service_id', 'trip_id']
    trips_rows = []
    TRIPS = {}

    indexed_schedules = {}
    all_schedules_ids = []
    for schedule in ds_compl['schedule']:
        indexed_schedules[schedule['id']] = schedule

    for sid in all_schedules_ids:
        schedule = indexed_schedules[sid]

        for route in routes:
            if route['name'] != schedule['route']['nameroute']: continue
            if route['abbrev'] != schedule['route']['abbrev']: continue
            route_id = route['id']
        
        trip_id = sid
        trip = [route_id, SERVICE_ID, sid]
        TRIPS[sid] = trip
        trips_rows.append(trip)

    scroutes = defaultdict(lambda: [])
    for schedule in ds_compl['schedule']:
        scroutes[schedule['route']['id']].append((schedule['timetable']['starttime'], schedule))

    for k, v in scroutes.items():
        scroutes[k].sort(key=lambda v: v[0])

    for srtid, schlist in scroutes.items():
        schedule = schlist[0][1]
        trip_id = srtid
        for route in routes:
            if route['name'] != schedule['route']['nameroute']: continue
            if route['abbrev'] != schedule['route']['abbrev']: continue
            route_id = route['id']

        trip = [route_id, SERVICE_ID, trip_id]
        TRIPS[trip_id] = trip
        trips_rows.append(trip)


    # generating stop times
    stop_times_head = ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence', 'stop_headsign']
    stop_times_rows = []

    for trip in trips_rows:
        trip_id = trip[2]
        counter = 0
        schlist = scroutes[trip_id]
        for sch in [s[1] for s in schlist]:
            now = datetime.datetime.fromtimestamp(1692824400)
            today = datetime.datetime(now.year, now.month, now.day, tzinfo=datetime.timezone(datetime.timedelta(hours=3)))
            arrival = (today + datetime.timedelta(minutes=float(sch['timetable']['starttime']))).strftime('%H:%M:%S')
            departure = (today + datetime.timedelta(minutes=float(sch['timetable']['endtime']))).strftime('%H:%M:%S')
            stop_sequence = counter
            berth_letter = sch['changed_schedule']['berth_letter'] or sch['position']['berth_letter']
            stop_id = f"{sch['dock']['id']}_{berth_letter}"
            ship_id = sch['changed_schedule']['ship_id'] or sch['ship']['id']
            ship_name = sch['changed_schedule']['ship_name'] or sch['ship']['name']
            stop_headsign = f"{ship_id}_{ship_name}"

            enstop = [trip_id, arrival, departure, stop_id, stop_sequence, stop_headsign]
            stop_times_rows.append(enstop)

            counter += 1


    fileWriter(calendar_head, [calendar_row], f'./{folder}/calendar.txt')
    fileWriter(stops_head, stops_rows, f'./{folder}/stops.txt')
    fileWriter(routes_head, routes_rows, f'./{folder}/routes.txt')
    fileWriter(stop_times_head, stop_times_rows, f'./{folder}/stop_times.txt')
    fileWriter(trips_head, trips_rows, f'./{folder}/trips.txt')


    shutil.make_archive(f'./{folder}', 'zip', f'./{folder}')

ds_docks = json.load(open('./dataset/docks.json', encoding="utf8"))
ds_schedule = json.load(open('./dataset/schedule.json', encoding="utf8"))
ds_ships = json.load(open('./dataset/ships.json', encoding="utf8"))

ds_compl = {'docks': ds_docks, 'schedule': ds_schedule, 'ships': ds_ships}
compileGTFS(ds_compl)