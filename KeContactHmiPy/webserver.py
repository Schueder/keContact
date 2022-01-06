from http.server import BaseHTTPRequestHandler, HTTPServer

class GP(BaseHTTPRequestHandler):
    
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()
        
    def do_GET(self):
        global current
           
    
def run(server_class=HTTPServer, handler_class=GP, port=8088):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    
    httpd.serve_forever()


run()
