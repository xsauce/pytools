# -*- coding: utf-8 -*-
import argparse
import logging
import logging.handlers
import sys
import traceback
try:
    from cloghandler import ConcurrentRotatingFileHandler as RFHandler
except ImportError:
    # Next 2 lines are optional:  issue a warning to the user
    from warnings import warn
    warn("ConcurrentLogHandler package not installed.  Using builtin log handler")
    from logging.handlers import RotatingFileHandler as RFHandler

sys.path.append('/usr/local/pt_anteater_paw/')
from util.base_handler import WorkingRequestCounter
import os
import functools
import signal
import time
import tornado.web
import tornado.ioloop
import tornado.log
import tornado.httpclient
from tornado.web import url
from handler import pt_handler, test_handler, wife_handler, moxie_handler, ctcf_loan_handler, tongdun_handler, madai_bank_deposit_handler
from handler.pt import loan_handler, shebao_hander, yys_handler, gjj_handler, bank_wap_handler, jd_handler, alipay_handler, credit_rank_handler
from util import base_handler
import settings
from settings import TORNADO_LOG_SETTINGS
import json
import tornado.httpserver
# import tornado.curl_httpclient.CurlAsyncHTTPClient

IS_STOPPING = False

def get_url_routes():
    handler_kw = 'Handler'
    ur = []
    handlers = [
        base_handler,
        pt_handler,
        test_handler,
        wife_handler,
        moxie_handler,
        bank_wap_handler,
        gjj_handler,
        yys_handler,
        shebao_hander,
        ctcf_loan_handler,
        loan_handler,
        tongdun_handler,
        alipay_handler,
        jd_handler,
        credit_rank_handler,
        madai_bank_deposit_handler
    ]
    # if settings.HOSTNAME in settings.PROD_HOSTNAMES:
    #     handlers.remove(ctcf_loan_handler)
    #     handlers.remove(loan_handler)

    for handler in handlers:
        for i in dir(handler):
            if i.endswith(handler_kw) and i[:-1 * len(handler_kw)] not in ['Base', 'Request']:
                handler_cls = getattr(handler, i)
                ur.append(url(handler_cls.ROUTE, handler_cls, handler_cls.ROUTE_PARAM, name=i[:-1 * len(handler_kw)]))
    return ur


def configuration_logging(**kwargs):
    log_dir = TORNADO_LOG_SETTINGS['log_file_prefix']
    TORNADO_LOG_SETTINGS.update(**kwargs)
    port = settings.TORNADO_SERVER_SETTINGS['port']
    for log_name, logger in [('tornado_access_%s.log' % port, tornado.log.access_log), ('tornado_app_%s.log' % port, tornado.log.app_log), ('tornado_gen_%s.log' % port, tornado.log.gen_log)]:
        log_file = os.path.join(log_dir, log_name)
        logger.setLevel(getattr(logging, TORNADO_LOG_SETTINGS['logging'].upper()))
        if log_file:
            rotate_mode = TORNADO_LOG_SETTINGS['log_rotate_mode']
            if rotate_mode == 'size':
                channel = RFHandler(log_file, "a", TORNADO_LOG_SETTINGS['log_file_max_size'], TORNADO_LOG_SETTINGS['log_file_num_backups'])
                #
                # channel = logging.handlers.RotatingFileHandler(
                #     filename=log_file,
                #     maxBytes=TORNADO_LOG_SETTINGS['log_file_max_size'],
                #     backupCount=TORNADO_LOG_SETTINGS['log_file_num_backups']
                # )
            elif rotate_mode == 'time':
                channel = logging.handlers.TimedRotatingFileHandler(
                    filename=log_file,
                    when=TORNADO_LOG_SETTINGS['log_rotate_when'],
                    interval=TORNADO_LOG_SETTINGS['log_rotate_interval'],
                    backupCount=TORNADO_LOG_SETTINGS['log_file_num_backups'])
            else:
                error_message = 'The value of log_rotate_mode option should be ' + \
                                '"size" or "time", not "%s".' % rotate_mode
                raise ValueError(error_message)
            channel.setFormatter(tornado.log.LogFormatter(fmt=TORNADO_LOG_SETTINGS['log_fmt'], datefmt=TORNADO_LOG_SETTINGS['log_datefmt'], color=False))
            logger.addHandler(channel)

        if (TORNADO_LOG_SETTINGS['log_to_stderr'] or (TORNADO_LOG_SETTINGS['log_to_stderr'] is None and not logger.handlers)):
            # Set up color if we are in a tty and curses is installed
            channel = logging.StreamHandler()
            channel.setFormatter(tornado.log.LogFormatter(fmt=TORNADO_LOG_SETTINGS['log_fmt'], datefmt=TORNADO_LOG_SETTINGS['log_datefmt']))
            logger.addHandler(channel)


def pt_log_function(handler):
    pass

def sig_handler(server, sig, frame):
    tornado.log.app_log.info('catch_signal %s', sig)
    global IS_STOPPING
    if not IS_STOPPING:
        IS_STOPPING = True
        tornado.ioloop.IOLoop.current().add_callback_from_signal(functools.partial(shutdown, server))

def shutdown(server):
    tornado.log.app_log.info('start to shutdown server')
    server.stop() # 不接收新的 HTTP 请求
    tornado.log.app_log.info('server do not accept new request')
    tornado.log.app_log.info('will shutdown in %s seconds', settings.TORNADO_SERVER_SETTINGS['wait_seconds_before_shutdown'])
    io_loop = tornado.ioloop.IOLoop.current()
    deadline = time.time() + settings.TORNADO_SERVER_SETTINGS['wait_seconds_before_shutdown']

    def stop_loop():
        now = io_loop.time()
        working_request_count = WorkingRequestCounter().get_working_request_count()
        # tornado.log.app_log.info('%s, %s, %s, %s' % (working_request_count, now < deadline, io_loop._callbacks, str([(time.strftime("%Y:%m:%d %H:%M:%S", time.localtime(i.deadline)), (i.callback.func.__name__ if i.callback else None)) for i in io_loop._timeouts])))
        # 方案一 ： 到了时间直接关闭服务 (一般总会存在 null_wrapper 的 _timeout)
        # if now < deadline and (io_loop._callbacks or io_loop._timeouts):
        # 方案二 ： 当没有为 wrapped 的_timeout时，关闭服务 （仍然有可能中断未完成的请求, 方案需要优化)
        # if (io_loop._callbacks or [timeout for timeout in io_loop._timeouts if not (timeout.callback is None or timeout.callback.func.__name__=='null_wrapper')]):
        if working_request_count > 0 and now < deadline:
            tornado.log.app_log.info('%d working requests' % working_request_count)
            io_loop.add_timeout(now + 1, stop_loop)
        else:
            io_loop.stop() # 处理完现有的 callback 和 timeout 后，可以跳出 io_loop.start() 里的循环

    stop_loop()


def start_server(port=None):
    try:
        settings.TORNADO_SERVER_SETTINGS['port'] = port
        configuration_logging()
        for i in dir(settings):
            if isinstance(getattr(settings, i), dict):
                if not i.startswith('_'):
                    tornado.log.app_log.debug('%s:%s' % (i, json.dumps(getattr(settings, i))))

        tornado_application_settings = settings.TORNADO_APPLICATION_SETTINGS
        tornado_application_settings['log_function'] = pt_log_function

        app = tornado.web.Application(get_url_routes(), **tornado_application_settings)
        server = tornado.httpserver.HTTPServer(app)
        partial_sig_handler = functools.partial(sig_handler, server)
        signal.signal(signal.SIGTERM, partial_sig_handler)
        signal.signal(signal.SIGINT, partial_sig_handler)
        if settings.TORNADO_SERVER_SETTINGS['multi_mode']:
            server.bind(settings.TORNADO_SERVER_SETTINGS['port'])
            server.start(settings.TORNADO_SERVER_SETTINGS['subprocess_num'])
        else:
            app.listen(settings.TORNADO_SERVER_SETTINGS['port'], address=settings.TORNADO_SERVER_SETTINGS['ip'])
        # util.imap.async_imap_client.AsyncIMAPClient.configure(
        #     'util.imap.simple_imap_client.SimpleAsyncIMAPClient',
        #     max_clients=settings.TORNADO_SERVER_SETTINGS['async_max_imap_client'],
        #     in_queue_timeout=settings.TORNADO_SERVER_SETTINGS['async_imap_in_queue_timeout'],
        #     request_timeout=settings.TORNADO_SERVER_SETTINGS['async_imap_request_timeout'],
        #     connect_timeout=settings.TORNADO_SERVER_SETTINGS['async_imap_connect_timeout'])
        tornado.httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient",
                                                     max_clients=settings.TORNADO_SERVER_SETTINGS[
                                                         'async_max_http_client'])
        tornado.log.app_log.info('start server,listen %s:%s' % (settings.TORNADO_SERVER_SETTINGS['ip'], port))
        tornado.ioloop.IOLoop.current().start()
        tornado.log.app_log.info('finish to shutdown server')
    except SystemExit as e:
        if str(e) == '0':
            tornado.log.app_log.info('parent process exited normally')
        else:
            tornado.log.app_log.error('parent process exited by SystemExit(%s)' % e)
    except:
        # if e.args == (SystemExit, 0):
        #     tornado.log.app_log.info('parent process exit normally')
        # else:
        tornado.log.app_log.error('terminate server %s, %s', json.dumps(traceback.format_exception(*sys.exc_info())))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', help='port num', default='9090')
    args = parser.parse_args()
    start_server(args.port)
