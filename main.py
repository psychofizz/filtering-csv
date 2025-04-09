import traceback
import pandas as pd
import sys

usecols= ["timestamp","metric_name", "metric_value","method", "status", "url"]

class Metrics:
    def __init__(self):
        self.http_req_duration = None
        self.http_req_connecting = None
        self.http_req_waiting = None
        self.http_req_receiving = None  
        self.timestamp = None
        self.metric_value = None
        self.method = None
        self.status = None
        self.url = None
    def __str__(self):
        metrics = [
            f"HTTP Request Duration: {self.http_req_duration}",
            f"HTTP Request Connecting: {self.http_req_connecting}",
            f"HTTP Request Waiting: {self.http_req_waiting}",
            f"HTTP Request Receiving: {self.http_req_receiving}",
            f"Timestamp: {self.timestamp}",
            f"Metric Value: {self.metric_value}",
            f"Method: {self.method}",
            f"Status: {self.status}",
            f"URL: {self.url}"
        ]
        return "\n".join(metrics)


def process_http_metrics(input_file, output_file):
    try:
        metrics = Metrics()
        iteration = 0
        csv_data = pd.read_csv(input_file, low_memory=False, usecols=usecols)
##      csv_data.to_csv('csv_data', encoding='utf-8'
    
        


        for x in range (100):
            row_counter = x % 9          

            metrics.timestamp = csv_data.iloc[x]['timestamp']
            metrics.metric_value = csv_data.iloc[x]['metric_value']
            metrics.method = csv_data.iloc[x]['method']
            metrics.status = csv_data.iloc[x]['status']
            metrics.url = csv_data.iloc[x]['url']

            if "http_req_duration" == csv_data.iloc[x]['metric_name']:
                metrics.http_req_duration = csv_data.iloc[x]['metric_value']

            if "http_req_connecting" == csv_data.iloc[x]['metric_name']:
                metrics.http_req_connecting = csv_data.iloc[x]['metric_value']

            if "http_req_waiting" == csv_data.iloc[x]['metric_name']:
                metrics.http_req_waiting = csv_data.iloc[x]['metric_value']

            if "http_req_receiving" == csv_data.iloc[x]['metric_name']:
                metrics.http_req_receiving = csv_data.iloc[x]['metric_value']


            if row_counter == 0 and x != 0:
                print("")
                print(metrics)
                metrics = Metrics()
                


        

##        for x in range(csv_data.size):
##            http_req_duration = csv_data[x,3]
##            if iteration % 8 == 0 and iteration != 0:
            
            

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