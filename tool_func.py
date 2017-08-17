# -*- coding: utf-8 -*-
import StringIO
import calendar
import copy
import cPickle as pickle
import datetime
import hmac
import json
import os
import re
import uuid

import math

import decimal
import psutil
from Crypto.Cipher import AES
from Crypto import Random
import base64
import settings
import hashlib
import pytz
from dateutil.parser import parse as timezone_parse
from util import oss2_helper


class ShortUUID:
    def __init__(self):
        self._uuid = None

    def get_original_uuid(self):
        return self._uuid_obj

    def _to_short(self, uuid_obj):
        self._uuid_obj = uuid_obj
        return uuid_obj.bytes.encode('base64').rstrip('=\n').replace('/', '_')

    def uuid1_str(self, *args, **kwargs):
        return self._to_short(uuid.uuid1(*args, **kwargs))

    def uuid3_str(self, *args, **kwargs):
        return self._to_short(uuid.uuid3(*args, **kwargs))

    def uuid4_str(self):
        return self._to_short(uuid.uuid4())

    def uuid5_str(self, *args, **kwargs):
        return self._to_short(uuid.uuid5(*args, **kwargs))

    def to_uuid(self, shorten_uuid_str):
        return uuid.UUID(bytes=(shorten_uuid_str + '==').replace('_', '/').decode('base64'))


class CallbackContent:
    def __init__(self, content):
        self._content = ''
        self._content_ossfile = ''
        self.set(content)

    def get_full_ossfile(self):
        return 'https://sys-python.oss-cn-shanghai.aliyuncs.com/' + self._content_ossfile

    def get(self):
        if self._content == '':
            return oss2_helper.Oss2Helper.getDefaultOss2Helper().getObjectStream(self._content_ossfile).read()
        else:
            return self._content

    def set(self, content):
        # if isinstance(content, unicode):
        #     content = bytes(content)
        # if len(content) > 1024:
            # 1kB
        self._content_ossfile = '%s/%s/%s.json' % (settings.OSS_PREFIX['callback_tmp'], datetime.datetime.now().strftime('%Y%m%d'), str(uuid.uuid1()))
        oss2_helper.Oss2Helper.getDefaultOss2Helper().putObject(self._content_ossfile, content)
        # else:
        #     self._content = content


def lose_log_content(content):
    if content is None:
        return content
    if len(content) >= 60000:
        ossfile_name = '%s/%s/%s' % (
        settings.OSS_PREFIX['err_log'], datetime.datetime.now().strftime('%Y%m%d'), str(uuid.uuid1()))
        oss2_helper.Oss2Helper.getDefaultOss2Helper().putObject(ossfile_name, content)
        return ossfile_name
    else:
        return content


def dict_to_obj(d):
    class A:
        pass

    for k, v in d.items():
        setattr(A, k, v)
    return A()

def encode_app_user_id(app_user_id):
    return md5(base64.b64encode(str(app_user_id).zfill(20)))

def calculate_age(today, born):
    try:
        birthday = born.replace(year=today.year)
    except ValueError:
        # raised when birth date is February 29
        # and the current year is not a leap year
        birthday = born.replace(year=today.year, day=born.day-1)
    if birthday > today:
        return today.year - born.year - 1
    else:
        return today.year - born.year

def obj_dump(obj):
    obj_str = StringIO.StringIO()
    pickle.dump(obj, obj_str)
    return obj_str


def obj_load(obj_str):
    return pickle.load(obj_str)

def to_str(s):
    return unicode(s) if s is not None else ''


def dict_to_jsonstr(d, **kwargs):
    class _MyJsonEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, datetime.datetime):
                return o.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(o, datetime.date):
                return o.strftime('%Y-%m-%d')
            elif isinstance(o, decimal.Decimal):
                return float(o)
            else:
                return json.JSONEncoder.default(self, o)
    return json.dumps(d, cls=_MyJsonEncoder, **kwargs)

class DateUtil(object):
    @staticmethod
    def add_month(dt, months):
        month = dt.month - 1 + months
        year = int(dt.year + month / 12)
        month = month % 12 + 1
        day = min(dt.day, calendar.monthrange(year, month)[1])
        new_dt = copy.copy(dt).replace(year=year, month=month, day=day)
        return new_dt


def process_list(attr_list=['pid', 'name', 'cmdline']):
    pinfos = []
    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=attr_list)
        except psutil.NoSuchProcess:
            pass
        else:
            pinfos.append(pinfo)
    return pinfos

def base64_hmac_sha256(secret, bytes_unicode):
    secret = unicode(secret).encode('utf-8')
    bytes_unicode = bytes_unicode.encode('utf-8')
    sign = base64.b64encode(hmac.new(secret, bytes_unicode, digestmod=hashlib.sha256).digest())
    return sign.decode('utf-8')

def convert_to_money_chinese(num):
    capUnit = [u'万', u'亿', u'万', u'元', '']
    capDigit = {2: [u'角', u'分', ''], 4: [u'仟', u'佰', u'拾', '']}
    capNum = [u'零', u'壹', u'贰', u'叁', u'肆', u'伍', u'陆', u'柒', u'捌', u'玖']
    snum = str('%019.02f') % num
    if snum.index('.') > 16:
        return ''
    ret, nodeNum, subret, subChr = '', '', '', ''
    CurChr = ['', '']
    for i in range(5):
        j = int(i * 4 + math.floor(i / 4))
        subret = ''
        nodeNum = snum[j:j + 4]
        lens = len(nodeNum)
        for k in range(lens):
            if int(nodeNum[k:]) == 0:
                continue
            CurChr[k % 2] = capNum[int(nodeNum[k:k + 1])]
            if nodeNum[k:k + 1] != '0':
                CurChr[k % 2] += capDigit[lens][k]
            if not ((CurChr[0] == CurChr[1]) and (CurChr[0] == capNum[0])):
                if not ((CurChr[k % 2] == capNum[0]) and (subret == '') and (ret == '')):
                    subret += CurChr[k % 2]
        subChr = [subret, subret + capUnit[i]][subret != '']
        if not ((subChr == capNum[0]) and (ret == '')):
            ret += subChr
    result = [ret, capNum[0] + capUnit[3]][ret == '']
    if not result.endswith(u'分'):
        return result + u'整'
    else:
        return result

def camel_to_pythonist(word):
    pythonist_word = []
    big_a = ord('A')
    big_z = ord('Z')
    for index, c in enumerate(word):
        if big_a <= ord(c) <= big_z:
            if index > 0:
                pythonist_word.append('_' + c.lower())
            else:
                pythonist_word.append(c.lower())
        else:
            pythonist_word.append(c)
    return ''.join(pythonist_word)



class AESCipher:
    BS = 16

    @staticmethod
    def get_default():
        return AESCipher(settings.AESKEY)

    def __init__(self, key):
        self.key = key

    def encrypt(self, raw):
        pad = lambda s: s + (self.BS - len(s) % self.BS) * chr(self.BS - len(s) % self.BS)
        raw = pad(raw)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(raw))

    def decrypt(self, enc):
        unpad = lambda s: s[:-ord(s[len(s) - 1:])]
        enc = base64.b64decode(enc)
        iv = enc[:16]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(enc[16:]))

def md5(text):
    m = hashlib.md5()
    if isinstance(text, unicode):
        text = text.encode('utf-8')
    m.update(text)
    return m.hexdigest()

def guess_gender_by_id_card(id_card):
    if not id_card:
        return None
    if isinstance(id_card, int):
        id_card = str(id_card)
    match_result = re.match('^([\d\*]{15}|[\d\*]{18})$', id_card)
    if match_result is None:
        return None
    if len(id_card) == 18:
        gender_signal = str(id_card)[16]
    elif len(id_card) == 15:
        gender_signal = str(id_card)[14]
    else:
        gender_signal = None
    if gender_signal == '*':
        return None
    if int(gender_signal) % 2 == 0:
        return u'女'
    else:
        return u'男'

def second_to_hhmmss(second):
    second = int(second)
    if second >= 24 * 60 * 60:
        raise Exception('second <= 24 * 60 * 60')
    hour = second // 3600
    minute = (second % 3600) // 60
    second = (second % 3600) % 60

    return '%02d:%02d:%02d' % (hour, minute, second)


def save_tmp_file(file_name, content):
    with open(os.path.join(settings.TMP_DIR, file_name), 'w') as f:
        f.write(content)

def id_card_18to15(id_card_18):
    id_card_15 = list(copy.copy(id_card_18))
    id_card_15.pop(6)
    id_card_15.pop(6)
    id_card_15.pop(15)
    return ''.join(id_card_15)

def id_card_15to18(id_card_15):
    id_card_15 = copy.copy(id_card_15)
    id_card_18 = id_card_15[:6] + '19' + id_card_15[6:]
    w = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
    v = ['1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2']
    tmp_t = 0
    for i, n in enumerate(map(int, list(id_card_18))):
        tmp_t += w[i] * n
    id_card_18 += v[tmp_t % 11]
    return id_card_18

def to_utc8_timestr(timestr):
    """ 将带时区的时间字符串 转出 不带时区的东八区时间字符串
    """
    tz = pytz.timezone('Asia/Shanghai')
    return datetime.datetime.strftime(timezone_parse(timestr).astimezone(tz), '%Y-%m-%d %H:%M:%S')


def validate_id_card(id_card):
    # 校验位和生日位
    id_card = id_card.strip().upper()
    if len(id_card) == 18:
        try:
            birthday = datetime.datetime.strptime(id_card[6:14], '%Y%m%d')
            w = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
            v = ['1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2']
            tmp_t = 0
            for i, n in enumerate(map(int, id_card[:-1])):
                tmp_t += n * w[i]
            if id_card[-1].upper() == v[tmp_t % 11]:
                return 1
        except:
            return 0
    elif len(id_card) == 15:
        return validate_id_card(id_card_15to18(id_card))
    else:
        return 0

def time33(s, seed=5381):
    h = seed
    for c in s:
        h += (h << 5) + ord(c)
    return h

def get_child_table_key(key, child_table_count=128):
    assert isinstance(key, int), 'key must be int'
    assert child_table_count > 0, 'child_table_count must be bigger than 0'
    s = hex(key).lstrip('0x')
    # s_list = list(s)
    # for i in range(len(s)):
    #     ord_s = ord(s_list[i])
    #     if 48 <= ord_s <= 57:
    #         s_list[i] = chr(53 + ord_s)
    # v = time33(''.join(s_list))
    v = time33(s)
    return v % child_table_count
