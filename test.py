import threading
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import requests

def fetch_and_save_page(url, output_file):
    # Function to fetch and save page content
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_file, "a") as file:
            file.write(response.text + '\n')
        # Extract the URL of the next page if available
        next_page_url = extract_next_page_url(response)
        return next_page_url
    else:
        print("Error: Unable to fetch page at", url)
        return None

def extract_next_page_url(response):
    # Dummy function to extract the URL of the next page
    # Replace this with your actual implementation
    # For example, you can parse the response content to find the URL of the next page
    return 'https://example.com/NextPage'

def assign_workers(second_page_url):
    # Function to assign workers for processing subsequent pages
    parsed_url = urlparse(second_page_url)
    query_params = parse_qs(parsed_url.query)
    current_offset_value = int(query_params.get('_getpagesoffset', [0])[0])
    batch_size = int(query_params.get('_count', [0])[0])
    if '_getpagesoffset' in query_params:
        del query_params['_getpagesoffset']
    params_no_offset = urlencode(query_params, doseq=True)
    next_url_format = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, params_no_offset, parsed_url.fragment))

    end_reached = False  # Variable to track if the end is reached
    lock = threading.Lock()  # Lock to synchronize access to current_offset_value

    def worker():
        nonlocal current_offset_value, end_reached
        while not end_reached:
            with lock:
                worker_offset = current_offset_value
                current_offset_value += batch_size
                # Construct URL with current offset
                worker_url = next_url_format.replace("{offset}", str(worker_offset))
            page_content = fetch_and_save_page(worker_url, output_file)
            if page_content is None:
                print("Worker stopped: Unable to fetch page at", worker_url)
                return
            print("Worker processed:", page_content)

    # Start 4 worker threads
    worker_threads = []
    for _ in range(4):
        thread = threading.Thread(target=worker)
        thread.start()
        worker_threads.append(thread)

    # Wait for all worker threads to complete
    for thread in worker_threads:
        thread.join()

if __name__ == "__main__":
    initial_query_url = "https://example.com/Patient?_lastUpdated=gt2020-01-01"
    output_file = "output.ndjson"
    next_page_url = fetch_and_save_page(initial_query_url, output_file)
    if next_page_url:
        assign_workers(next_page_url)
    else:
        print("No next page URL available. Done.")
