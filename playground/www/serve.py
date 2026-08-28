#!/usr/bin/env python3
"""Static server for the playground: HTTP Range + CORS + WebDAV PROPFIND.

`python3 -m http.server` is not enough. It answers a Range request with a
full 200, and object_store correctly refuses to read parquet off a host that
cannot do partial reads. This one sends 206 with Content-Range, which is what
S3, MinIO and RustFS all do.

Usage: python3 serve.py [port]
"""

import email.utils
import os
import re
import sys
import urllib.parse
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
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS, PROPFIND")
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

    def do_PROPFIND(self):
        """Directory listing, the way object_store's HTTP store asks for it.

        A URL ending in "/" makes DataFusion list rather than stat, and
        object_store lists an HTTP source over WebDAV. Without this the server
        answers 501, object_store reads that as retryable, and a directory
        source hangs for the whole retry budget instead of failing.
        """
        path = self.translate_path(self.path)
        if not os.path.isdir(path):
            self.send_error(404, "Not a collection")
            return

        base = self.path if self.path.endswith("/") else self.path + "/"
        entries = [(base, os.stat(path), True)]
        depth = self.headers.get("Depth", "1")
        if depth != "0":
            for name in sorted(os.listdir(path)):
                full = os.path.join(path, name)
                if os.path.isfile(full):
                    entries.append((base + urllib.parse.quote(name), os.stat(full), False))

        out = ['<?xml version="1.0" encoding="utf-8"?>',
               '<D:multistatus xmlns:D="DAV:">']
        for href, st, is_dir in entries:
            # object_store requires getlastmodified on EVERY response, the
            # collection included, or it rejects the whole document with
            # "missing field `getlastmodified`".
            mtime = email.utils.formatdate(st.st_mtime, usegmt=True)
            if is_dir:
                props = (f"<D:resourcetype><D:collection/></D:resourcetype>"
                         f"<D:getlastmodified>{mtime}</D:getlastmodified>")
            else:
                props = (f"<D:resourcetype/>"
                         f"<D:getcontentlength>{st.st_size}</D:getcontentlength>"
                         f"<D:getlastmodified>{mtime}</D:getlastmodified>")
            out.append(f"<D:response><D:href>{href}</D:href><D:propstat>"
                       f"<D:prop>{props}</D:prop>"
                       f"<D:status>HTTP/1.1 200 OK</D:status>"
                       f"</D:propstat></D:response>")
        out.append("</D:multistatus>")
        body = "\n".join(out).encode()

        self.send_response(207, "Multi-Status")
        self.send_header("Content-Type", 'application/xml; charset="utf-8"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

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
