import os
import threading

from queue import Queue, Empty

import pandas as pd 
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

from config import NO_ADDRESS_TRAJ_DIR, NOMINATIM_PATH, PROXY

CURRENT_CITY = os.environ.get("AGENTMOVE_CITY", "Shanghai")


def reverse_geocode(lat, lon):
    geolocator = Nominatim(user_agent="MyGeocodingApp2",timeout=10,proxies=PROXY if os.environ.get("AGENTMOVE_USE_PROXY") else None)
    geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)
    location = geocode((lat, lon), exactly_one=True,language='en')
    return location.address if location else None

def process_map(city, venue_city):
    venue_address = {}
    for venue, coord in venue_city.items():
        if venue not in venue_address:
            lat, lon = coord[1], coord[0]
            address = reverse_geocode(lat, lon)
            venue_address[venue] = address
            
            if address:
                item = {
                    "city": city,
                    "venue_id": venue,
                    "lng": lon,
                    "lat": lat,
                    "address": address,
                }
                q.put(item)
            
    return venue_address


def save_worker():
    with open(os.path.join(NOMINATIM_PATH, '{}.csv'.format(CURRENT_CITY)), 'a') as f:
        if f.tell() == 0:
            f.write('city\tvenue_id\tlng\tlat\taddress\n')
        while running or not q.empty():
            try:  
                item = q.get(block=True, timeout=10)
                print(item)
                f.write(f'{item["city"]}\t{item["venue_id"]}\t{item["lng"]}\t{item["lat"]}\t{item["address"]}\n')
                q.task_done()
            except Empty:
                continue


if __name__ == '__main__':
    q = Queue()
    running = True
    
    os.makedirs(NOMINATIM_PATH, exist_ok=True)
    thread = threading.Thread(target=save_worker, daemon=True)
    thread.start()
    
    venue_map = {}
    cities = []
    print("reading city files...")
    for file in os.listdir(NO_ADDRESS_TRAJ_DIR):
        fs = pd.read_csv(os.path.join(NO_ADDRESS_TRAJ_DIR, file))
        city = file.split("_filtered")[0]
        print(f"processing city {city}...")

        if city != CURRENT_CITY:
            continue

        cities.append(city)
        venue_map[city] = {}
        for _,row in fs.iterrows():
            venue = row['venue_id']
            lon = row['longitude']
            lat = row['latitude']
            if venue not in venue_map[city]:
                venue_map[city][venue] = (lon, lat)
        
    for city,items in venue_map.items():
        print(city,len(items))

    for city in cities:
        venue_address = process_map(city, venue_map[city])
    
    running = False
    thread.join()
