#!/usr/bin/env python3
"""Static server for the playground: HTTP Range + CORS + brotli/gzip.

`python3 -m http.server` is not enough. It answers a Range request with a
full 200, and object_store correctly refuses to read parquet off a host that
cannot do partial reads. This one sends 206 with Content-Range, which is what
S3, MinIO and RustFS all do.

Usage: python3 serve.py [port]
"""

import os
import re
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))


class Handler(SimpleHTTPRequestHandler):
    extensions_map = {
        **SimpleHTTPRequestHandler.extensions_map,
        ".wasm": "application/wasm",
        ".js": "text/javascript",
    }

    def __init__(self, *a, **kw):
        super().__init__(*a, directory=ROOT, **kw)

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header(
            "Access-Control-Expose-Headers",
            "content-range,content-length,etag,last-modified,accept-ranges",
        )
        self.send_header("Accept-Ranges", "bytes")
        # The wasm bundle is large and immutable per build; let the browser
        # keep it so a reload is a boot, not a download.
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def send_head(self):
        rng = self.headers.get("Range")
        if not rng:
            return super().send_head()

        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            return super().send_head()

        m = re.match(r"bytes=(\d*)-(\d*)$", rng.strip())
        if not m:
            self.send_error(416, "Malformed Range")
            return None

        size = os.path.getsize(path)
        start_s, end_s = m.group(1), m.group(2)
        if start_s:
            start = int(start_s)
            end = int(end_s) if end_s else size - 1
        else:
            # Suffix form: "bytes=-N" means the last N bytes. Parquet readers
            # use this to grab the footer before they know the file length.
            start = max(0, size - int(end_s or 0))
            end = size - 1
        end = min(end, size - 1)

        if start > end or start >= size:
            self.send_response(416)
            self.send_header("Content-Range", f"bytes */{size}")
            self.end_headers()
            return None

        length = end - start + 1
        self.send_response(206)
        self.send_header("Content-Type", self.guess_type(path))
        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(length))
        self.end_headers()

        with open(path, "rb") as f:
            f.seek(start)
            self.wfile.write(f.read(length))
        return None

    def log_message(self, format, *args):  # noqa: A002 - matches the base signature
        sys.stderr.write("  %s\n" % (format % args))


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8799
    print(f"\n  SQE playground  ->  http://127.0.0.1:{port}\n")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
