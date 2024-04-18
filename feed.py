# read line from log-1000.txt and POST it to http://localhost:4000/v1/influxdb/write?db=public&precision=ms
# POST data format: "many_logs,host=1 log=<FILE CONTENT> <INCREMENT ID>"

import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

batch_size = 3000
worker = 8

# Define the URL
url = "http://localhost:4000/v1/influxdb/write?db=public&precision=ms"


def send_data(start, data):
    # Send the POST request
    response = requests.post(url, data=data)
    # Check the response
    if response.status_code >= 300:
        print(
            f"Failed to send log line {start}: {response.status_code} {response.text}"
        )


# Open the file
with open("target/log-1000.txt", "r") as file:
    lines = file.readlines()

# Create a progress bar
with tqdm(
    total=len(lines),
    desc="Processing lines",
    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
) as pbar:
    data = ""
    with ThreadPoolExecutor(max_workers=worker) as executor:
        for i, line in enumerate(lines):
            # Prepare the POST data
            content = line.strip()
            content = content.replace('"', " ")
            content = content.replace("'", " ")
            content = content.replace("=", " ")
            content = content.replace(".", " ")

            data = data + f'many_logs,host=1 log="{content}" {i}\n'

            if i % batch_size == 0:
                executor.submit(send_data, i, data)
                data = ""
                # Update the progress bar
                pbar.update(batch_size)

        # close the executor
        executor.shutdown(wait=True)
