#!/usr/bin/env python3
"""Tiny JSON echo upstream used by the reverse-proxy demos/tests."""
import http.server
import json
import sys


class Handler(http.server.BaseHTTPRequestHandler):
    def _handle(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = json.dumps({
            "upstream": "ok",
            "method": self.command,
            "path": self.path,
            "echo_body": self.rfile.read(length).decode("utf-8", "replace") if length else "",
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = _handle

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18090
    http.server.HTTPServer(("127.0.0.1", port), Handler).serve_forever()
