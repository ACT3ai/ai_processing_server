"""A fake object store standing in for S3 presigned URLs: serves input files and receives uploaded outputs.

GET  /files/<name>          returns a registered file (inputs)
PUT  /bucket/<name>?sig=... stores the body (outputs), recording headers
Failures can be injected: fail_next_gets(n) answers the next n GETs with 503 (a transient error).
"""

import hashlib
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


class FakeStorage:
    def __init__(self):
        self.files = {}      # name -> bytes
        self.uploads = {}    # name -> {"body": bytes, "headers": dict, "query": str}
        self.gets = []       # names requested, in order
        self._fail_gets = 0
        self._lock = threading.Lock()
        storage = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_GET(self):
                name = urlparse(self.path).path.removeprefix("/files/")
                with storage._lock:
                    storage.gets.append(name)
                    if storage._fail_gets > 0:
                        storage._fail_gets -= 1
                        return self._send(503, b"try again")
                body = storage.files.get(name)
                if body is None:
                    return self._send(404, b"no such file")
                self._send(200, body)

            def do_PUT(self):
                u = urlparse(self.path)
                name = u.path.removeprefix("/bucket/")
                n = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(n)
                with storage._lock:
                    storage.uploads[name] = {"body": body, "headers": dict(self.headers), "query": u.query}
                self._send(200, b"")

            def _send(self, code, body):
                self.send_response(code)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def add_file(self, name, body):
        self.files[name] = body
        return {"name": name, "url": f"{self.base}/files/{name}", "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}

    def upload_url(self, name, secret="s3cr3t-signature"):
        """A presigned-looking URL; the signature must never appear in API responses or logs."""
        return f"{self.base}/bucket/{name}?X-Amz-Signature={secret}&sig={secret}"

    def fail_next_gets(self, n):
        with self._lock:
            self._fail_gets = n

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
