import traceback
import pandas as pd
import sys
import csv
import os
from datetime import datetime
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

usecols = ["timestamp", "metric_name", "metric_value", "method", "status", "url", "expected_response"]
write_lock = Lock()  # for thread-safe file writes


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
            f"ID: {self.id}",
            f"Result: {self.result}"
        ]
        return "\n".join(metrics)


def unix_to_date(unix_timestamp: int) -> str:
    return datetime.fromtimestamp(unix_timestamp).isoformat()


def process_chunk(chunk_data, chunk_id, output_file):
    try:
        # Group HTTP metrics by timestamp, method, status, url
        http_metrics = {}
        
        for _, row in chunk_data.iterrows():
            if not re.match(r'^http_', row['metric_name']):
                continue
            
            key = (row['timestamp'], row['method'], row['status'], row['url'])
            
            if key not in http_metrics:
                http_metrics[key] = {
                    'timestamp': row['timestamp'],
                    'method': row['method'],
                    'status': row['status'],
                    'url': row['url'],
                    'result': row['expected_response'],
                    'metrics': {}
                }
            
            # Store metric value by its name
            http_metrics[key]['metrics'][row['metric_name']] = row['metric_value']
        
        # Process each unique request
        for key, data in http_metrics.items():
            metrics = Metrics()
            metrics.id = chunk_id
            metrics.timestamp = unix_to_date(data['timestamp'])
            metrics.method = data['method']
            metrics.status = data['status']
            metrics.url = data['url']
            metrics.result = data['result']
            
            # Assign metric values if they exist
            metrics.http_req_duration = data['metrics'].get('http_req_duration')
            metrics.http_req_connecting = data['metrics'].get('http_req_connecting')
            metrics.http_req_waiting = data['metrics'].get('http_req_waiting')
            metrics.http_req_receiving = data['metrics'].get('http_req_receiving')
            
            # Ensure all metrics have values before saving
            if all([
                metrics.http_req_duration is not None,
                metrics.http_req_connecting is not None,
                metrics.http_req_waiting is not None,
                metrics.http_req_receiving is not None
            ]):
                ##print(f"\nProcessed ID {chunk_id}")
                ##print(metrics)
                
                with write_lock:
                    save_data(metrics, output_file)
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


def process_http_metrics(input_file, output_file, max_workers=4):
    try:
        df = pd.read_csv(input_file, low_memory=False, usecols=usecols)

        chunks = []
        for i in range(0, df.shape[0], 9):  # group by 9 rows (1 group = 1 request)
            chunk = df.iloc[i:i+9]
            chunks.append((chunk, i // 9))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_chunk, chunk_data, chunk_id, output_file)
                for chunk_data, chunk_id in chunks
            ]
            for future in as_completed(futures):
                future.result()

        return True

    except Exception:
        traceback.print_exc()
        return False


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python threaded_main.py archivo_entrada.csv archivo_salida.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    success = process_http_metrics(input_file, output_file)
    if not success:
        sys.exit(1)
