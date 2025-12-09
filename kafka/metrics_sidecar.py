import time
from prometheus_client import start_http_server


if __name__ == "__main__":
    # Expose default process metrics for the broker container.
    start_http_server(8001)
    while True:
        time.sleep(30)
