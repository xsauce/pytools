import re

import tornado.web, tornado.log
import collections, datetime, sys, json, traceback, abc, settings
import tornado.gen, tornado.template
import util.pbccrc
from pt_exceptions import ThirdAPIResponseError, NormalError
from pt_exceptions import WrongParameterError
from util import tool_func
from util.pt_tornado_logging import PTTornadoLogger, TYPE_C2P

class RspCode:
    NO_ERROR = 0
    # DEFAULT_ERROR = 100
    WRONG_PARAM_ERROR = MISSING_PARAM_ERROR = 301
    SYS_ERROR = 200
    WRONG_THIRD_RESPONSE_ERROR = 201
    THIRD_PARTY_ERROR = 300

    def __init__(self):
        pass

import threading

_WORKING_REQUEST_COUNT = set()

class WorkingRequestCounter:
    def __init__(self):
        self._set_lock = threading.Lock()

    def add(self, request_handler):
        global _WORKING_REQUEST_COUNT
        with self._set_lock:
            _WORKING_REQUEST_COUNT.add(request_handler)

    def remove(self, request_handler):
        global _WORKING_REQUEST_COUNT
        with self._set_lock:
            _WORKING_REQUEST_COUNT.remove(request_handler)

    def get_working_request_count(self):
        global _WORKING_REQUEST_COUNT
        return len(_WORKING_REQUEST_COUNT)


class BadArgumentException(Exception):
    def __init__(self, msg, code=102):
        super(BadArgumentException, self).__init__(self, msg, code)
        self.code = code
        self.message = msg

class BaseHandler(tornado.web.RequestHandler):
    ROUTE = ''
    ROUTE_PARAM = {}
    ASYNC_ACTION_LIST = []

    def data_received(self, chunk):
        pass

    def __init__(self, *args, **kwargs):
        super(BaseHandler, self).__init__(*args, **kwargs)

    def get_argument_in_range(self, name, enum_list, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if val in enum_list:
            return val
        elif val == default:
            return val
        else:
            raise BadArgumentException(name + ' is not in enum list')

    def get_argument_least_lenght(self, name, least_length, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if len(unicode(val)) >= least_length:
            return val
        elif val == default:
            return val
        else:
            raise BadArgumentException('length of %s < %s' % (name, least_length))

    def get_argument_id_no(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if val is default:
            return val
        try:
            is_match = re.match(r'^(\d{15}$|^\d{18}$|^\d{17}(\d|X|x))$', val).group()
            return val
        except:
            raise BadArgumentException(name + ' is invalid id_no')

    def get_argument_mobile(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if val is default:
            return val
        try:
            is_match = re.match(r'^1\d{10}$', val).group()
            return val
        except:
            raise BadArgumentException(name + ' is invalid mobile')

    def get_argument_credit_card_no(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if val is default:
            return val
        try:
            is_match = re.match(r'^\d{10,30}$', val).group()
            return val
        except:
            raise BadArgumentException(name + ' is invalid credit_card_no')

    def get_argument_float(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if val is default:
            return val
        try:
            return str(float(val))
        except:
            raise BadArgumentException(name + " should be of type float")

    def get_argument_int(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if val is default:
            return val
        try:
            return str(int(val))
        except:
            raise BadArgumentException(name + " should be of type int")

    def get_argument_email(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if val is default:
            return val
        val_split_list = val.split('@')
        if len(val_split_list) < 2:
            raise BadArgumentException("wrong email format: {0}".format(name))
        if len(val_split_list[0]) < 1 or len(val_split_list[1]) < 1:
            raise BadArgumentException("wrong email format: {0}".format(name))
        return val

    def get_argument_datetime(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if val is default:
            return val
        try:
            return datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
        except:
            raise BadArgumentException('wrong date format: {0}'.format(name))

    def get_argument_date(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if val is default:
            return val
        try:
            return datetime.datetime.strptime(val, '%Y-%m-%d')
        except:
            raise BadArgumentException('wrong date format: {0}'.format(name))

    def get_argument_ip(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        val = self.get_argument(name, default=default, strip=strip)
        if val is default:
            return val
        try:
            ip_array = val.split('.')
            if len(ip_array) < 4:
                raise
            else:
                for i in ip_array:
                    if not (0 <= int(i) <= 255):
                        raise
        except:
            raise BadArgumentException('wrong date format: {0}'.format(name))
        else:
            return val

    def get_arguments_atleastone(self, names):
        key_value_dict = {}
        all_none_bool = True
        for name in names:
            value = self.get_argument(name, None)
            key_value_dict[name] = value
            if value is not None:
                all_none_bool = False
        if all_none_bool:
            raise BadArgumentException('these keys can not be all None: '+ ','.join(key_value_dict.keys()))
        else:
            return key_value_dict

    def is_async_action(self, action):
        return action in self.ASYNC_ACTION_LIST

    @tornado.gen.coroutine
    def get(self, action=''):
        if self.is_async_action(action):
            yield self._action_handler(action)
        else:
            self._action_handler(action)

    @tornado.gen.coroutine
    def post(self, action=''):
        if self.is_async_action(action):
            yield self._action_handler(action)
        else:
            self._action_handler(action)

    def _format_error_msg(self, err):
        err_cls = unicode(err.__class__).split('.')[-1].strip("'>")
        return unicode(err_cls), unicode(err)

    def prepare(self):
        WorkingRequestCounter().add(self)

    def on_finish(self):
        WorkingRequestCounter().remove(self)

    @tornado.gen.coroutine
    def _action_handler(self, action):
        req_body, message = '', ''
        try:
            if self.request.headers.get("Content-Type", '').startswith("application/json"):
                req_body = json.dumps(json.loads(self.request.body))
            else:
                req_body = json.dumps(self.request.arguments)
        except Exception as e:
            req_body = 'parse_req_body_error:%s' % unicode(e)
        PTTornadoLogger(
            log_type=TYPE_C2P, url=self.request.uri, status_code='',
            req_body=req_body, resp_body='', cost_time=''
        ).info(message)
        try:
            action_method_name = '%s_action' % action
            if not hasattr(self, action_method_name):
                self.err_404(action)
                return
            action_method = getattr(self, action_method_name)
            if self.is_async_action(action):
                yield action_method()
            else:
                action_method()
        except tornado.web.MissingArgumentError as e:
            self.output_json(code=RspCode.MISSING_PARAM_ERROR, msg=unicode(e))
        except BadArgumentException as e:
            self.output_json(code=RspCode.WRONG_PARAM_ERROR, msg=e.message)
        except ThirdAPIResponseError as e:
            message = json.dumps(traceback.format_exception(*sys.exc_info()))
            self.output_json(code=RspCode.WRONG_THIRD_RESPONSE_ERROR, msg=e.output_message)
        except WrongParameterError as e:
            self.output_json(code=RspCode.THIRD_PARTY_ERROR, msg=e.output_message)
        except NormalError as e:
            self.output_json(code=RspCode.THIRD_PARTY_ERROR, msg=e.output_message, data=e.data)
        except util.pbccrc.PbccrcException as e:
            self.output_json(code=RspCode.THIRD_PARTY_ERROR, msg=unicode(e))
        except Exception as e:
            message = json.dumps(traceback.format_exception(*sys.exc_info()))
            err_msg = self._format_error_msg(e)
            self.output_json(code=RspCode.SYS_ERROR, msg=err_msg[1], debug=err_msg[0])
        finally:
            if message != '':
                PTTornadoLogger(
                    log_type=TYPE_C2P, url=self.request.uri,
                    status_code=self.get_status(), req_body=req_body,
                    method=self.request.method,
                    resp_body=''.join(self._write_buffer), cost_time='%.2f' % (self.request.request_time() * 1000)
                ).error(message)
            else:
                PTTornadoLogger(
                    log_type=TYPE_C2P, url=self.request.uri,
                    status_code=self.get_status(), req_body=req_body,
                    method=self.request.method,
                    resp_body=''.join(self._write_buffer), cost_time='%.2f' % (self.request.request_time() * 1000)
                ).info(message)


    def err_404(self, action):
        self.set_status(404, '%s not found' % action)
        self.write('Page Not Found')

    def output_raw_response(self, status_code, reason='', msg=''):
        self.set_status(status_code, reason)
        self.write(msg)

    def output_json(self, data=None, msg=u'success', code=0, debug=None):
        self.set_header("Content-Type", "application/json")
        self.write(tool_func.dict_to_jsonstr(collections.OrderedDict([('code', code), ('msg', msg), ('data', data),('debug', debug)])))

    def output_template(self, path, file_name):
        self.set_header("Content-Type", "text/html")
        self.write(tornado.template.Loader(settings.ROOT_DIR+'/template/'+path).load(file_name).generate())

class PwStaticFileHandler(tornado.web.StaticFileHandler):
    ROUTE = r'/static/(.*)'
    ROUTE_PARAM = {'path': settings.ROOT_DIR + '/static'}