import json
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class SSEHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args):
        # Reduce noise; override default logging
        return

    def do_GET(self):
        if self.path != "/events/stream":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        event_id = 0
        try:
            while True:
                event_id += 1
                user_id = random.randint(1, 5)
                event = {
                    "event_id": event_id,
                    "user_id": user_id,
                    "timestamp": int(time.time())
                }
                payload = f"data: {json.dumps(event)}\n\n".encode("utf-8")
                self.wfile.write(payload)
                self.wfile.flush()
                time.sleep(1)
        except (BrokenPipeError, ConnectionResetError):
            # client disconnected
            pass


def run(host: str = "127.0.0.1", port: int = 8080):
    server = ThreadingHTTPServer((host, port), SSEHandler)
    print(f"Mock SSE server running at http://{host}:{port}/events/stream")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    run()
