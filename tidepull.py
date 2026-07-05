import requests


def get_noaa_tide_data(station_id, station_name):
    # NOAA CO-OPS API Data Retrieval URL
    url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

    # API Parameters
    params = {
        "date": "today",          # Fetch data for the current day
        "station": station_id,
        "product": "predictions",
        "datum": "mllw",          # Mean Lower Low Water reference
        "time_zone": "lst_ldt",   # Use local time
        "interval": "hilo",       # Show only High and Low tide peaks
        "units": "english",       # Results in feet
        "format": "json"
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        print(f"\n{'='*45}")
        print(f"STATION: {station_name} ({station_id})")
        print(f"{'Time':<20} | {'Type':<6} | {'Level (ft)'}")
        print("-" * 45)
        if "predictions" in data:
            for pred in data["predictions"]:
                t_time = pred["t"]
                t_type = "High" if pred["type"] == "H" else "Low"
                t_level = pred["v"]
                print(f"{t_time:<20} | {t_type:<6} | {t_level}")
        else:
            print("Notice: No tide predictions found for this station today.")

    except requests.exceptions.RequestException as e:
        print(f"Error connecting to NOAA for {station_name}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# Station IDs matched to regionConfig.locations
stations = {
    "8652659": "Oregon Inlet Bridge",          # Oregon Inlet, NC
    "8654467": "USCG Station Hatteras",        # Hatteras Inlet, NC
    "8656483": "Beaufort, Duke Marine Lab",    # Beaufort Inlet, NC
    "8637689": "Gloucester Point, VA",         # Poquoson, VA
    "8638863": "Cape Henry, VA",               # Bay Bridge Tunnel, VA / Virginia Beach, VA
    "8570283": "Ocean City, MD",               # Ocean City Inlet, MD
}

if __name__ == "__main__":
    for s_id, s_name in stations.items():
        get_noaa_tide_data(s_id, s_name)
