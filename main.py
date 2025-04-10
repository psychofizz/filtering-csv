import traceback
import pandas as pd
import sys
import csv
import os
from datetime import datetime
import re

usecols= ["timestamp","metric_name", "metric_value","method", "status", "url", "expected_response"]

class Metrics:
    def __init__(self):
        self.http_req_duration = None
        self.http_req_connecting = None
        self.http_req_waiting = None
        self.http_req_receiving = None 
        self.timestamp = None
        self.method = None
        self.status = None
        self.url = None
        self.id = None
        self.result = None
    def __str__(self):
        metrics = [
            f"HTTP Request Duration: {self.http_req_duration}",
            f"HTTP Request Connecting: {self.http_req_connecting}",
            f"HTTP Request Waiting: {self.http_req_waiting}",
            f"HTTP Request Receiving: {self.http_req_receiving}",
            f"Timestamp: {self.timestamp}",
            f"Method: {self.method}",
            f"Status: {self.status}",
            f"URL: {self.url}",
            f"ID: {self.id}"
            f"Result: {self.result}"
        ]
        return "\n".join(metrics)


def unix_to_date(unix_timestamp: int) -> str:
    return datetime.fromtimestamp(unix_timestamp).isoformat()

def process_http_metrics(input_file, output_file):
    try:
        metrics = Metrics()
        iteration = 0
        csv_data = pd.read_csv(input_file, low_memory=False, usecols=usecols)
    
        for x in range (csv_data.shape[0]):
            ## to skip things that arent http metrics
            if not re.match(r'^http_', csv_data.iloc[x]['metric_name']):
                print ("Skipping non-http metric")
                continue
                
            row_counter = x % 9          

            metrics.timestamp = unix_to_date(csv_data.iloc[x]['timestamp'])
            metrics.method = csv_data.iloc[x]['method']
            metrics.status = csv_data.iloc[x]['status']
            metrics.url = csv_data.iloc[x]['url']
            metrics.result = csv_data.iloc[x]['expected_response']

            if "http_req_duration" == csv_data.iloc[x]['metric_name']:
                metrics.http_req_duration = csv_data.iloc[x]['metric_value']

            if "http_req_connecting" == csv_data.iloc[x]['metric_name']:
                metrics.http_req_connecting = csv_data.iloc[x]['metric_value']

            if "http_req_waiting" == csv_data.iloc[x]['metric_name']:
                metrics.http_req_waiting = csv_data.iloc[x]['metric_value']

            if "http_req_receiving" == csv_data.iloc[x]['metric_name']:
                metrics.http_req_receiving = csv_data.iloc[x]['metric_value']

            if row_counter == 0 and x != 0:
                metrics.id = iteration
                print("")
                print(metrics)
                iteration = iteration + 1
                save_data(metrics, output_file=output_file)
                
    except Exception:
        traceback.print_exc()

                
                
            
def save_data(metrics, output_file):
    headers = [
        "ID",
        "HTTP Request Duration",
        "HTTP Request Connecting",
        "HTTP Request Waiting",
        "HTTP Request Receiving",
        "Timestamp",
        "Method",
        "Status",
        "URL",
        "Check"
    ]

    try:
        file_exists = os.path.isfile(output_file)
        is_empty = not file_exists or os.path.getsize(output_file) == 0

        with open(output_file, 'a', newline='') as file:
            writer = csv.writer(file)

            if is_empty:
                writer.writerow(headers)

            writer.writerow([
                metrics.id,
                metrics.http_req_duration,
                metrics.http_req_connecting,
                metrics.http_req_waiting,
                metrics.http_req_receiving,
                metrics.timestamp,
                metrics.method,
                metrics.status,
                metrics.url,
                metrics.result
            ])

    except Exception:
        traceback.print_exc()



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python script.py archivo_entrada.csv archivo_salida.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    
    success = process_http_metrics(input_file, output_file)
    if not success:
        sys.exit(1)