#!/usr/bin/env python3
# Tiny HTTP fixture for oj-readiness-smoke.sh S3 (PR #14 http:// scheme).
# Serves the current directory on the port given as argv[1], bound to 0.0.0.0
# so containers reach it via host.docker.internal.
import sys, http.server, socketserver
port = int(sys.argv[1]) if len(sys.argv) > 1 else 18077
with socketserver.TCPServer(("0.0.0.0", port), http.server.SimpleHTTPRequestHandler) as httpd:
    httpd.serve_forever()
