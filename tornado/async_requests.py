import multiprocessing
import rfc822
from cStringIO import StringIO

import datetime
import requests
import requests.structures
import requests.cookies
import sys
import tornado.gen
import tornado.httpclient
from tornado.ioloop import IOLoop
import tornado.curl_httpclient
import urlparse

class _MockTornadoHttpResponse:
    def __init__(self, header):
        s = StringIO()
        s.write(str(header))
        s.seek(0)
        self._headers = rfc822.Message(s)

    def info(self):
        return self._headers


class TornadoRequestSession(requests.Session):
    '''
    add async request function based on requests.Session, it can maintain cookie state.
    async request do not support all the params (just headers, cookies, files, data, json), other params do not work.
    '''
    def __init__(self):
        super(TornadoRequestSession, self).__init__()
        self.current_request_wait_time = None

    def add_session_cookies(self, name, value, domain='', path='/', expires=None, secure=False):
        new_cookie = requests.cookies.create_cookie(name, value, domain=domain, path=path, expires=expires, secure=secure)
        self.cookies.set_cookie(new_cookie)

    @tornado.gen.coroutine
    def async_request(self, method, url,
                    params=None,
                    data=None,
                    headers=None,
                    cookies=None,
                    files=None,
                    auth=None,
                    timeout=None,
                    allow_redirects=True,
                    proxies=None,
                    hooks=None,
                    stream=None,
                    verify=None,
                    cert=None,
                    json=None,
                    request_timeout=None
                    ):

        http_client = tornado.httpclient.AsyncHTTPClient()
        # no support params
        params, auth, hooks, stream, verify, cert = None, None, None, None, None, None
        req = requests.Request(
            method=method.upper(),
            url=url,
            headers=headers,
            files=files,
            data=data or {},
            json=json,
            params=params or {},
            auth=auth,
            cookies=cookies,
            hooks=hooks,
        )
        prepared_req = self.prepare_request(req)
        if proxies is not None:
            proxy_url = None
            for k, v in proxies.iteritems():
                if k in prepared_req.url:
                    proxy_url = v
            parse_result = urlparse.urlparse(proxy_url)
            async_request = tornado.httpclient.HTTPRequest(url=prepared_req.url, method=prepared_req.method,
                                                           headers=prepared_req.headers, body=prepared_req.body,
                                                           request_timeout=request_timeout,
                                                           allow_nonstandard_methods=True, proxy_host=parse_result.hostname, proxy_port=parse_result.port,proxy_username=parse_result.username,proxy_password=parse_result.password)
        else:
            async_request = tornado.httpclient.HTTPRequest(url=prepared_req.url, method=prepared_req.method,
                                                           request_timeout=request_timeout, headers=prepared_req.headers, body=prepared_req.body, allow_nonstandard_methods=True)
        start = datetime.datetime.utcnow()
        async_response = yield http_client.fetch(async_request)
        self.current_request_wait_time = async_response.time_info.get('queue', 0)
        response = requests.Response()
        response.elapsed = async_response.request_time
        response.status_code = async_response.code
        response.headers = requests.structures.CaseInsensitiveDict(async_response.headers)
        response.encoding = requests.utils.get_encoding_from_headers(response.headers)
        response.raw = async_response.buffer
        response.reason = async_response.reason
        response.url = prepared_req.url
        # Add new cookies from the server.
        mock_req = requests.cookies.MockRequest(prepared_req)
        mock_res = _MockTornadoHttpResponse(async_response.headers)
        self.cookies.extract_cookies(mock_res, mock_req)
        response.cookies.extract_cookies(mock_res, mock_req)
        # Give the Response some context.
        response.request = prepared_req
        if not stream:
            response.content
        raise tornado.gen.Return(response)

    @tornado.gen.coroutine
    def async_post(self, url, data=None, json=None, request_timeout=None, **kwargs):
        response = yield self.async_request('POST', url, data=data, json=json, request_timeout=request_timeout, **kwargs)
        raise tornado.gen.Return(response)

    @tornado.gen.coroutine
    def async_get(self, url, data=None, json=None, request_timeout=None, **kwargs):
        response = yield self.async_request('GET', url, data=data, json=json, request_timeout=request_timeout, **kwargs)
        raise tornado.gen.Return(response)

@tornado.gen.coroutine
def async_f():
    r = []
    st = str(datetime.datetime.now())
    tornado_session = TornadoRequestSession()
    response = yield tornado_session.async_get('http://127.0.0.1:8889')
    response = yield tornado_session.async_post('http://127.0.0.1:8889')
    r.append(tornado_session.cookies)
    r.append(st)
    print r
    raise tornado.gen.Return(r)

def sync_f(i):
    r = []
    session = requests.Session()
    st = str(datetime.datetime.now())
    resp = session.get('http://127.0.0.1:8889')
    resp = session.post('http://127.0.0.1:8889')
    r.append(session.cookies)
    r.append(st)
    r.append(i)
    print r
    return r

#
# def sync_test():
#     r = multiprocessing.Pool(processes=10).imap(sync_f, range(10))
#     for i in r:
#         print i
#         sys.stdout.flush()

@tornado.gen.coroutine
def async_proxy_test():
    s = TornadoRequestSession()
    # response = yield s.async_post('http://cn.bing.com/', proxies={'http://cn.bing.com':'http://139.196.75.13:8080'})
    response = yield s.async_post('http://cn.bing.com/')
    print response.content
    print 1

if __name__ == '__main__':
    # import time
    #
    # st = time.time()
    # sync_f(1)
    # print int(time.time() - st)
    #
    # st = time.time()
    # IOLoop().current().run_sync(async_f)
    # print int(time.time() - st)
    async_proxy_test()