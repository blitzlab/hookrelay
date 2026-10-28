"""A deliberately unreliable webhook receiver for local demos.

It fails a configurable share of requests, verifies signatures when given the
endpoint secret, and dedupes on Webhook-Id to show how a receiver turns
at-least-once delivery into exactly-once processing.

    python scripts/flaky_receiver.py --port 9000 --failure-rate 0.3 --secret whsec_...
"""

import argparse
import json
import random
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from apps.webhooks.services.signing import SIGNATURE_HEADER, SignatureError, verify_signature  # noqa: E402

processed: set[str] = set()
lock = threading.Lock()
stats = {"received": 0, "failed_on_purpose": 0, "duplicates": 0, "processed": 0, "bad_signature": 0}


def make_handler(failure_rate: float, secret: str | None):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self._reply(200, stats)

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            with lock:
                stats["received"] += 1

            if secret:
                try:
                    verify_signature(secret, body, self.headers.get(SIGNATURE_HEADER))
                except SignatureError as exc:
                    with lock:
                        stats["bad_signature"] += 1
                    return self._reply(400, {"error": str(exc)})

            if random.random() < failure_rate:
                with lock:
                    stats["failed_on_purpose"] += 1
                return self._reply(503, {"error": "simulated outage"})

            event_id = self.headers.get("Webhook-Id", "")
            with lock:
                if event_id in processed:
                    stats["duplicates"] += 1
                else:
                    processed.add(event_id)
                    stats["processed"] += 1
            self._reply(200, {"ok": True})

        def _reply(self, code: int, payload: dict):
            data = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt, *args):
            sys.stdout.write(f"receiver {self.address_string()} {fmt % args}\n")

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--failure-rate", type=float, default=0.3)
    parser.add_argument("--secret", default=None, help="endpoint secret used to verify signatures")
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), make_handler(args.failure_rate, args.secret))
    print(f"flaky receiver on :{args.port} failure_rate={args.failure_rate} (GET / for stats)", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
