import sys
import aiohttp
import asyncio
import time
import json

from aiohttp import web

server_relations = {
    'Bailey': ['Bona', 'Campbell'],
    'Bona': ['Bailey', 'Clark', 'Campbell'],
    'Campbell': ['Bailey', 'Bona', 'Jaquez'],
    'Clark': ['Bona', 'Jaquez'],
    'Jaquez': ['Clark', 'Campbell']
}

server_ports = {
    'Bailey': 10000,
    'Bona': 10001,
    'Campbell': 10002,
    'Clark': 10003,
    'Jaquez': 10004
}
localhost = '127.0.0.1'

# dictionary that maps client_name to a list of [location, tsreceived, tssent, server_name]
client_information = {}

log_file = None

def log_message(message):
    if log_file:
        log_file.write(message + '\n')
        log_file.flush()

# GET request
gplace_api_key = 'VALID API KEY'
url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

async def fetch_data(location, radius, result_count):
    params = {
        'location': location, 
        'radius': radius, 
        'key': gplace_api_key}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()

    data["results"] = data.get("results",[])[:int(result_count)]
    return data


# check valid request

def validate_location(location):
    try:
        log_message(f"Validating location: {location}")
        if location.count('+') + location.count('-') != 2:
            return False
        log_message("Will being parsing location...")
        lat, lon = parse_location(location)
        log_message(f"Parsed location: {lat}, {lon}")

        # since parsed locations only returns the tuple or nothing...
        if lat is None or lon is None:
            return False

        return True
    except Exception as e:
        log_message(f"Error validating location: {e}")
        return False


# parse string (+lat-lon) into (+lat, -lon) tuples
def parse_location(location):
    log_message("Now parsing...")
    sign_index = []
    for i in range(len(location)):
        if location[i] == '+' or location[i] == '-':
            sign_index.append(i)

    # lat and lon should each only have 1 sign                                                                                       
    if len(sign_index) != 2:
        return None

    if sign_index[0] !=	0 or sign_index[1] == len(location) - 1:
        return None

    lat = location[sign_index[0]:sign_index[1]]
    lon = location[sign_index[1]:]

    try:
        if not (-90 <= float(lat) <= 90 and -180 <= float(lon) <= 180):
            return None
    except ValueError:
        log_message(f"Error converting lat or lon to float: lat={lat}, lon={lon}")
        return None

    return (lat, lon)

        
def check_valid(message):

    log_message("We made it to check valid")
    if len(message) < 1:
        return "Invalid"
    
    command_name = message[0]

    # IAMAT [client name] [location] [time] is 4 tokens long
    if command_name == "IAMAT":
        if len(message) == 4 and validate_location(message[2]):
            log_message("Returning IAMAT request type")
            return command_name
    
    # WHATSAT [client name] [radius] [results count] is 4 tokens long
    # results count should be less than 20
    if command_name == "WHATSAT":
        if len(message) == 4:
            try:
                radius = int(message[2])
                max_count = int(message[3])
                if radius > 0 and radius <= 50 and 0 < max_count <= 20:
                    log_message("Returning IAMAT request type")
                    return command_name
            except ValueError:
                return "Invalid"
            
    # UPDATE is 6 tokens long
    if command_name == "UPDATE":
        if len(message) == 6:
            return command_name

            
    return 'Invalid'


# handle client request 

async def handle_client(reader, writer):
    og_request = ''
    try:
        log_message("Waiting for read request...")

        request = await reader.readline()

        if not request:
            log_message("Client disconnected before sending full request")
            return
        
        og_request = request.decode()

        log_message(f"Received raw request: {og_request.strip()}")

        #received_requests is an array of strings
        received_request = og_request.strip().split()
   
        log_message("Received request: " + ' '.join(received_request))

        request_type = check_valid(received_request)
        
        response = ''

        # determine what kind of response

        # handle error
        if request_type == "Invalid":
            log_message("Invalid request type")
            response = '? ' + og_request
        # handle IAMAT
        elif request_type == "IAMAT":
            response = await handle_iamat(received_request)
        # handle WHATSAT
        elif request_type == "WHATSAT":
            response = await handle_whatsat(received_request)
        elif request_type == "UPDATE":
            log_message("Received UPDATE message. Sending to other servers...")
            response = await handle_update(received_request)


        if response:
            writer.write(response.encode())
            await writer.drain()
            log_message(f"Sent response: {response}")

        log_message("Closing connection!")

    # disconnect
    except asyncio.IncompleteReadError:
        log_message(f"Incomplete read error: {e}")
    except Exception as e:
        log_message(f"Unexpected error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

    
# Handling IAMAT
# client: IAMAT [client name] [location] [time]
async def handle_iamat(parts):        

    client_name = parts[1]
    location = parts[2]
    tssent = parts[3]

     # compute clock skew
    tsreceived = time.time()
    timestamp = str(float(tsreceived) - float(tssent))

    tsreceived = str(tsreceived)
    
    # if the difference is positive
    if timestamp[0] != '-':
        timestamp = '+' + timestamp

    repeat_msg = ' '.join(parts[1:])
    client_information[client_name] = [location, tsreceived, tssent, server_name] 
    log_message(f"Current client information: {client_information}")

    # response to IAMAT
    # server: AT [server name] [clock skew] [repeat msg]

    # repeat msg (everything from parts BUT the IAMAT)
    server_response = ' '.join(['AT', server_name, timestamp, repeat_msg]) + "\n"

    update_response = ' '.join(['UPDATE', client_name, location, tsreceived, tssent, server_name])

    log_message(f"Update message to send: {update_response}")
    
    await flood_update(update_response.strip())

    return server_response


# send message to all neighboring servers
async def flood_update(message):
    tasks = []
    log_message(f"Flooding update: {message}")
    for neighbor in server_relations.get(server_name, []):
        neighbor_port = server_ports[neighbor]
        tasks.append(asyncio.create_task(send_update(neighbor, neighbor_port, message)))

    await asyncio.gather(*tasks)


async def send_update(neighbor, port, message):
    try:
        reader, writer = await asyncio.open_connection(localhost, port)
        log_message(f"Forwarding update to {neighbor}:{port}")
        writer.write(message.encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()
    except (ConnectionRefusedError, asyncio.TimeoutError) as e:
        log_message(f"Warning: Failue to connect to {neighbor} at {port}. Error: {e}")

        
#Handling WHATSAT
# client: WHATSAT [client name] [radius] [results count]
async def handle_whatsat(parts):
    client_name = parts[1]
    radius = parts[2]
    result_count = parts[3]

    radius = str(int(radius) * 1000)

    # error handling
    if not radius.isdigit() or not result_count.isdigit():
        server_response = '?' + ' '.join(parts)
    if client_name not in client_information:
        server_response = '?' + ' '.join(parts)

    result_count = int(result_count)

    # server: AT [server name] [clock skew] [repeat msg] [places API call]
    log_message(f"Attempting to fetch client info for '{client_name}'...")

    if client_name in client_information:
        log_message(f"Found {client_name}! Data: {client_information[client_name]}")
    else:
        log_message(f"Not found '{client_name}' in client_information")
    
    location, tsreceived, tssent, server_name = client_information[client_name]
    log_message(f"Extracted info: loc={location}, ts_received={tsreceived}, ts_sent={tssent}, server={server_name}")
    
    lat, lon = parse_location(location)
    jlocation = lat + "," + lon
    jlocation = jlocation.replace("+","")

    timestamp = str(float(tsreceived) - float(tssent))

    # if the difference is positive
    if timestamp[0] != '-':
        timestamp = '+' + timestamp

    repeat_msg = ' '.join([client_name, location, tssent])
    at_response = ' '.join(['AT', server_name, timestamp, repeat_msg]) + '\n'

    log_message(f"Calling API with lat/lon: {location}")

    places_data = await fetch_data(jlocation, radius, result_count)
    places_json = json.dumps(places_data, indent=4)
    
    server_response = at_response + places_json + "\n"

    return server_response

# communcation between servers
# UPDATE [client_name][latest location][tsreceived][tssent][origin_server]
async def handle_update(parts):
    client_name = parts[1]
    latest_location = parts[2]
    tsreceived = parts[3]
    tssent = parts[4]
    origin_server = parts[5]

    update_msg = None

    log_message(f"Received UPDATE message from {origin_server}: {client_name} {latest_location} {tsreceived} {tssent}")
    

    # for comparison purposes
    f_tssent = float(tssent)

    # if client is known
    if client_name in client_information:
        client_tssent = float(client_information[client_name][2])

        if f_tssent > client_tssent:
            client_information[client_name] = parts[2:]
            update_msg = ' '.join(parts)

    else:
        client_information[client_name] = parts[2:]
        update_msg = ' '.join(parts)


    if update_msg is not None:
        await flood_update(update_msg.strip())
    else:
        log_message("No update needed, skipping flood.")

async def start_server(server_name):
    port = server_ports[server_name]
    print(f"Starting server {server_name} on port {port}...")  
    server = await asyncio.start_server(handle_client, localhost, port)
    
    print(f"Server {server_name} started successfully!")  
    
    async with server:
        print("Server is now accepting connections.")  
        await server.wait_closed()

def main():
    global server_name

    # proper command line call: python3 server.py [server_name]
    # takes two arguments
    if len(sys.argv) < 2:
        print("Proper command line: python3 server.py [server_name]")
        sys.exit(1)

    server_name = sys.argv[1]

    # set up logging
    log_file_name = f"{server_name}_log.txt"

    global log_file
    log_file = open(log_file_name, 'w+')
    log_file.truncate(0)

    try:
        asyncio.run(start_server(server_name))
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        if log_file:
            log_file.close()
 
if __name__ == "__main__":
    main()