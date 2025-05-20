# Version: Python 3.10.0

import os
import sys
import json
import csv
from matplotlib import pyplot as plt
"""
Usage: Usage: py analyzing.py [path/to]/simulation_log_[timestamp]
"""

def load_data(filepath: str):
    """
    Lädt Metadaten (.json) und CSV-Daten zu einer gegebenen Datei.
    Args: filepath (str): Pfad zur Basisdatei ohne Erweiterung (relativ oder absolut).
    Returns: tuple[dict, list[dict]]: (meta_data, csv_data)
    """
    # Absoluten Pfad bestimmen
    abs_filepath = os.path.abspath(filepath)
    base_path, base_name = os.path.split(abs_filepath)
    name, _ = os.path.splitext(base_name)
    json_path = os.path.join(base_path, f"{name}_meta.json")
    csv_path = os.path.join(base_path, f"{name}.csv")

    # JSON-Daten laden
    meta_data = {}
    try:
        with open(json_path, 'r', encoding='utf-8') as jf:
            meta_data = json.load(jf)
    except Exception as e:
        raise FileNotFoundError(f"Fehler beim Laden von {json_path}: {e}")

    # CSV-Daten laden
    csv_data = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as cf:
            reader = csv.DictReader(cf)
            csv_data = list(reader)
    except Exception as e:
        raise FileNotFoundError(f"Fehler beim Laden von {csv_path}: {e}")

    return meta_data, csv_data

def get_car_data_map(data):
    car_data_map = {}
    for dp in data:
        # Aufbau der car_data_struktur
        car_id = dp["car_id"]
        if dp["event_type"] == "ARRIVAL":
            car_data = {
                "arrival_time":int(dp["time"]),
                "departure_time":-1,
                "person_count":int(dp["person_count"])
            }
            car_data_map[car_id] = car_data
        elif dp["event_type"] == "DEPARTURE":
            car_data_map[car_id]["departure_time"] = int(dp["time"])
    return car_data_map

def get_car_data_statistics(car_data_map):
    total_people = 0
    total_arrivals = 0
    total_rejects = 0
    dwell_times = []
    for key in car_data_map:
        value = car_data_map[key]
        total_people += value["person_count"]
        total_arrivals += 1
        if value["departure_time"] == -1:
            total_rejects += 1
        else:
            dwell_times.append(value["departure_time"]-value["arrival_time"])
    return total_arrivals, total_rejects, total_people, dwell_times

def plot_dwell_time_distribution(dwell_times):
    plt.figure(figsize=(10, 6))
    plt.hist(dwell_times, bins=30, color='skyblue', edgecolor='black')
    plt.title("Dwell time distribution")
    plt.xlabel("Dwell Time (seconds)")
    plt.ylabel("Frequency")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.show()

def plot_cars_in_system_over_time(data):
    # Sortiere die Daten nach Zeit
    data.sort(key=lambda x: int(x['time']))

    # Erstelle eine Lookup-Liste für schnelleres Suchen
    
    time_points = [int(entry['time']) for entry in data]
    cars_in_sys_values = [entry['cars_in_sys'] for entry in data]

    max_time = max(time_points)
    step = 300  # 5 Minuten in Sekunden

    sampled_times = []
    sampled_counts = []

    for t in range(0, max_time + 1, step):
        # Finde das letzte Event vor Zeitpunkt t
        index = None
        for i in reversed(range(len(time_points))):
            if time_points[i] < t:
                index = i
                break
        if index is not None:
            sampled_times.append(t / 60)  # Minuten für bessere Lesbarkeit
            sampled_counts.append(cars_in_sys_values[index])
    # Kombiniere und sortiere die Daten nach Zeit
    combined = sorted(zip(sampled_times, sampled_counts))

    # Entpacke wieder in separate Listen
    sampled_times, sampled_counts = zip(*combined)
    # Plot
    plt.figure(figsize=(10, 6))
    plt.bar(sampled_times, sampled_counts, width=5, color='skyblue', edgecolor='black')
    plt.xlabel("Time (minutes)")
    plt.ylabel("Number of Cars in System")
    plt.title("Number of Cars in the System Over Time")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: py analyzing.py [path/to]/simulation_log_[timestamp]")
        sys.exit(1)
    filepath = sys.argv[1]
    try:
        meta_data, data = load_data(filepath)
        #print(data)
        print(f"Queue Size {meta_data['queue_limit']}")
        car_data_map = get_car_data_map(data)
        arrivals, rejects, people, dwell_times = get_car_data_statistics(car_data_map)
        for key in car_data_map:
            print(f"{key}:{car_data_map[key]}")
        print(f"Arrivals: {arrivals}, Rejects: {rejects}, Rejects %: {round((rejects*100.0)/arrivals,2)}%")
        print(f"Average People per car: {round((people*1.0)/arrivals)}")
        print(f"{dwell_times}")
        plot_dwell_time_distribution(dwell_times)
        plot_cars_in_system_over_time(data)

    except Exception as e:
        print(f"Fehler: {e}")
        sys.exit(1)
