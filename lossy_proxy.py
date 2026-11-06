"""Proxy HTTP que repassa a requisição e descarta as primeiras respostas.

O receptor processa a requisição normalmente. O proxy fecha a conexão
sem devolver a resposta, e o emissor não sabe se o evento chegou.

Uso: python lossy_proxy.py --port 8002 --upstream http://127.0.0.1:8001 --drop 1
"""

import argparse
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx


def build_handler(upstream, drop):
    remaining = [drop]
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            response = httpx.post(
                upstream + self.path,
                content=body,
                headers={"Content-Type": self.headers.get("Content-Type", "")},
            )
            with lock:
                lose = remaining[0] > 0
                remaining[0] -= lose
            if lose:
                print(f"proxy: {self.path} processado, resposta {response.status_code} descartada", flush=True)
                self.close_connection = True
                return
            self.send_response(response.status_code)
            self.send_header("Content-Type", response.headers.get("Content-Type", ""))
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

        def log_message(self, *args):
            pass

    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--drop", type=int, default=1)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), build_handler(args.upstream, args.drop))
    server.serve_forever()


if __name__ == "__main__":
    main()
