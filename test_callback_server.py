from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import urllib
import sys
import json


class CallbackHTTPHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        datas = self.rfile.read(int(self.headers['content-length']))
        datas = urllib.unquote(datas).decode("utf-8", 'ignore')
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        buf = json.dumps({"code": 0, "message": "success"})
        self.log_message('req:%s,resp:%s', datas, buf)
        self.wfile.write(buf)


def start_server(ipport):
    p = tuple(ipport.split(":"))
    http_server = HTTPServer((p[0], int(p[1])), CallbackHTTPHandler)
    print 'listen %s' % ipport
    http_server.serve_forever()


if __name__ == '__main__':
    start_server(sys.argv[1])
