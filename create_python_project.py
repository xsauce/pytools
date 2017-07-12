import argparse
import os


class ProjectCreater(object):
    def __init__(self, project_name, project_path):
        self.project_path = os.path.join(project_path, project_name)

    def _mkdir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def write_settings_py(self):
        setting_py_path = os.path.join(self.project_path, 'src', 'settings.py')
        if not os.path.exists(setting_py_path):
            with open(setting_py_path, 'w') as f:
                f.write('''import os
import sys
ROOT_PATH = os.path.dirname(__file__)
PRJ_ROOT_PATH = os.path.dirname(ROOT_PATH)
sys.path.append(ROOT_PATH)
LOG_PATH = os.path.join(os.path.dirname(ROOT_PATH), 'log')
if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)''')

    def write_util_log_py(self):
        util_log_py_path = os.path.join(self.project_path, 'src', 'util', 'log.py')
        if not os.path.exists(util_log_py_path):
            with open(util_log_py_path, 'w') as f:
                f.write('''import logging
import os
from logging.handlers import SMTPHandler
from cloghandler import ConcurrentRotatingFileHandler as RFHandler
import sys
from settings import LOG_PATH

def create_logger(logger_name,
                  log_level='INFO',
                  print_to_std=True,
                  print_to_file=True,
                  log_file='',
                  max_bytes_per_file=100 * 1024 * 1024,
                  backup_num=20,
                  log_format=''
    ):
    if print_to_file and not log_file:
        log_file = os.path.join(LOG_PATH, logger_name + '.log')

    logger = logging.getLogger(logger_name)

    log_level_num = getattr(logging, log_level)
    logger.setLevel(log_level_num)

    formatter = logging.Formatter(log_format or '%(asctime)s - %(levelname)s - %(message)s')

    if print_to_file:
        rotate_handler = RFHandler(log_file, "a", max_bytes_per_file, backup_num)
        rotate_handler.setFormatter(formatter)
        logger.addHandler(rotate_handler)
    if print_to_std:
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger


def create_email_logger(logger_name, log_level='INFO', log_format='', mailhost='smtp.163.com', fromaddr='singxle@163.com', toaddrs='singxle@163.com', subject='', credentials=None, secure=None, timeout=10):
    logger = logging.getLogger(logger_name)
    log_level_num = getattr(logging, log_level)
    logger.setLevel(log_level_num)
    formatter = logging.Formatter(log_format or '%(asctime)s - %(levelname)s - %(message)s')
    if not isinstance(toaddrs, list):
        toaddrs = [toaddrs]
    smtp_handler = SMTPHandler(mailhost, fromaddr, toaddrs, subject, credentials, secure=secure, timeout=timeout)
    smtp_handler.setFormatter(formatter)
    logger.addHandler(smtp_handler)
    return logger''')

    def create(self):
        if os.path.exists(self.project_path):
            continue_flag = raw_input('project path:%s exists, continue:(y/n)' % self.project_path)
            if not continue_flag or continue_flag.lower() == 'n':
                return
        else:
            os.makedirs(self.project_path)

        os.chdir(self.project_path)
        self._mkdir(os.path.join(self.project_path, 'src'))
        self._mkdir(os.path.join(self.project_path, 'src', 'util'))
        self._mkdir(os.path.join(self.project_path, 'log'))
        self._mkdir(os.path.join(self.project_path, 'shell'))
        self._mkdir(os.path.join(self.project_path, 'build'))
        if not os.path.exists(os.path.join(self.project_path, 'README.MD')):
            with open('README.MD', 'w') as f:
                f.write('')
        self.write_settings_py()
        self.write_util_log_py()
        print 'create', self.project_path, 'successfully'


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--pn', help='project name which you want to create')
    parser.add_argument('--pp', default=os.getcwd(), help='project path where you create')
    args = parser.parse_args()
    ProjectCreater(args.pn, args.pp).create()
