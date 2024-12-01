import logging
import azure.functions as func
from opensky_api import OpenSkyApi
import json
from azure.eventhub import EventHubProducerClient, EventData
import os

app = func.FunctionApp()

# Environment variables for Event Hub connection
EVENT_HUB_CONNECTION_STRING = os.environ["EVENT_HUB_CONNECTION_STRING"]
EVENT_HUB_NAME = os.environ["EVENT_HUB_NAME"]
OPEN_SKY_USERNAME = os.environ.get("OPEN_SKY_USERNAME")  # Optional for anonymous
OPEN_SKY_PASSWORD = os.environ.get("OPEN_SKY_PASSWORD")  # Optional for anonymous

# Total number of partitions in the Event Hub
EVENT_HUB_PARTITIONS = 8
# Initialize a global counter for round-robin partitioning
current_partition = 0

@app.timer_trigger(schedule="*/30 * * * * *", arg_name="flightTimer", run_on_startup=True, use_monitor=False) 
def timer_trigger(flightTimer: func.TimerRequest) -> None:
    global current_partition  # Use the global counter for round-robin logic
    logging.info("Azure Function triggered to poll OpenSky API using the SDK.")

    try:
        # Initialize OpenSky API
        api = OpenSkyApi(OPEN_SKY_USERNAME, OPEN_SKY_PASSWORD)

        # Fetch flight states
        states = api.get_states()

        if states and states.states:
            logging.info(f"Retrieved {len(states.states)} flight states from OpenSky Network.")

            # Send data to Event Hub
            current_partition = send_to_event_hub_round_robin(states.states, current_partition)
        else:
            logging.info("No flight states available from OpenSky Network.")

    except Exception as e:
        logging.error(f"Error polling OpenSky API using SDK: {e}")

def send_to_event_hub_round_robin(states, start_partition):
    """
    Sends flight states as individual messages to an Event Hub, 
    assigning them to partitions in a round-robin manner.
    """
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME
    )

    try:
        current_partition = start_partition  # Start from the given partition

        for state in states:
            # Serialize state object to JSON
            state_data = {
                "icao24": state.icao24,
                "callsign": state.callsign,
                "origin_country": state.origin_country,
                "time_position": state.time_position,
                "last_contact": state.last_contact,
                "longitude": state.longitude,
                "latitude": state.latitude,
                "baro_altitude": state.baro_altitude,
                "on_ground": state.on_ground,
                "velocity": state.velocity,
                "true_track": state.true_track,
                "vertical_rate": state.vertical_rate,
                "sensors": state.sensors,
                "geo_altitude": state.geo_altitude,
                "squawk": state.squawk,
                "spi": state.spi,
                "position_source": state.position_source,
            }
            message = json.dumps(state_data)
            event = EventData(message)

            # Send the message to the specified partition
            partition_id = str(current_partition)
            producer.send_batch([event], partition_id=partition_id)
            logging.info(f"Sent message to partition {partition_id}")

            # Update the partition for round-robin logic
            current_partition = (current_partition + 1) % EVENT_HUB_PARTITIONS

    except Exception as e:
        logging.error(f"Error sending messages to Event Hub: {e}")
    finally:
        producer.close()

    return current_partition  # Return the last partition used