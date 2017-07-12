# -*- coding: utf-8 -*-
import os
import logging
import argparse
from logging.handlers import TimedRotatingFileHandler
import sqlite3
import pymysql


class IncrSql(object):
    LOG_FILE = '/var/log/incr_sql/incr_sql.log'
    DB_FILE = '/var/log/incr_sql/incr_sql.db'

    def __init__(self, db_host, sqlpath, start_from, db_name, db_port, db_user, db_passwd, db_charset, init_sqlfile):
        self.db_host = db_host
        self.sqlpath = sqlpath
        self.start_from = start_from
        self.db_user = db_user
        self.db_port = db_port
        self.db_passwd = db_passwd
        self.db_name = db_name
        self.db_charset = db_charset
        self.init_sqlfile = init_sqlfile
        self.logger = self.create_logger('incr_sql')
        if not os.path.exists(os.path.dirname(self.LOG_FILE)):
            os.makedirs(os.path.dirname(self.LOG_FILE))
        if not os.path.exists(self.LOG_FILE):
            with open(self.LOG_FILE, 'w') as f:
                f.write('')
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect(self.DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS incr_sql(
            sqlpath TEXT NOT NULL,
            last_sql_file TEXT NOT NULL,
            db_host TEXT NOT NULL,
            db_name TEXT NOT NULL,
            PRIMARY KEY(sqlpath,db_host, db_name)
        )''')
        conn.commit()
        if not (self.get_last_sql_file() and self.init_sqlfile):
            cursor.execute("INSERT INTO incr_sql(last_sql_file, sqlpath, db_host, db_name) VALUES('%s', '%s', '%s', '%s')" % (self.init_sqlfile, self.sqlpath, self.db_host, self.db_name))
        conn.commit()
        conn.close()

    def get_last_sql_file(self):
        conn = sqlite3.connect(self.DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT last_sql_file from incr_sql where sqlpath='%s' and db_host='%s' and db_name='%s'" % (self.sqlpath, self.db_host, self.db_name))
        rows = cursor.fetchone()
        conn.commit()
        conn.close()
        return rows[0] if rows else ''

    def update_last_sql_file(self, last_sql_file):
        conn = sqlite3.connect(self.DB_FILE)
        if self.get_last_sql_file():
            sql = "UPDATE incr_sql SET last_sql_file='%s' WHERE sqlpath='%s' and db_host='%s' and db_name='%s'" % (last_sql_file, self.sqlpath, self.db_host, self.db_name)
        else:
            sql = "INSERT INTO incr_sql(last_sql_file, sqlpath, db_host, db_name) VALUES('%s', '%s', '%s', '%s')" % (last_sql_file, self.sqlpath, self.db_host, self.db_name)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
        except:
            conn.rollback()
        finally:
            conn.close()

    def create_logger(self, logger_name):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        time_handler = TimedRotatingFileHandler(self.LOG_FILE, when='d', backupCount=30)
        time_handler.setFormatter(formatter)
        logger.addHandler(time_handler)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        return logger

    def find_need_execute_sql(self):
        last_file = self.start_from or self.get_last_sql_file()
        self.logger.info('last_file=%s', last_file)
        sql_file_list = []
        for f in os.listdir(self.sqlpath):
            tmp_sql_file = os.path.join(self.sqlpath, f)
            if os.path.isfile(tmp_sql_file) and f.endswith('.sql') and f > last_file:
                sql_file_list.append(f)
        return sorted(sql_file_list)

    def execute_sql_one_file(self, sql_file):
        full_sql_file = os.path.join(self.sqlpath, sql_file)
        conn = pymysql.connect(host=self.db_host, port=self.db_port, user=self.db_user, password=self.db_passwd, db=self.db_name, charset=self.db_charset)
        try:
            with open(full_sql_file, 'r') as f:
                sql_script = f.read()
            if sql_script:
                with conn.cursor() as cursor:
                    for line in sql_script.split(';'):
                        if line.strip():
                            cursor.execute(line + ';')
            conn.commit()
            self.logger.info('execute %s', full_sql_file)
        except:
            self.logger.error('execute error %s', full_sql_file)
            conn.rollback()
            raise
        finally:
            conn.close()


    def run(self):
        self.logger.info('start db_host=%s, db_name=%s, sqlpath=%s start_from=%s', self.db_host, self.db_name, self.sqlpath, self.start_from)
        sql_file_list = self.find_need_execute_sql()
        executed_last_sql_file = ''
        try:
            for sql_file in sql_file_list:
                self.execute_sql_one_file(sql_file)
                executed_last_sql_file = sql_file
        finally:
            if sql_file_list and executed_last_sql_file:
                self.update_last_sql_file(executed_last_sql_file)
                self.logger.info('end update db_host=%s, db_name=%s, sqlpath=%s last_sql_file=%s ', self.db_host, self.db_name, self.sqlpath, executed_last_sql_file)
            else:
                self.logger.info('end do nothing db_host=%s, db_name=%s, sqlpath=%s', self.db_host, self.db_name, self.sqlpath)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sqlpath', type=str)
    parser.add_argument('--db_host', type=str)
    parser.add_argument('--db_name', type=str)
    parser.add_argument('--db_port', type=int, default=3306)
    parser.add_argument('--db_user', type=str, default='root')
    parser.add_argument('--db_passwd', type=str, default='1' * 6)
    parser.add_argument('--db_charset', type=str, default='utf8')
    parser.add_argument('--start_from', type=str, default='')
    parser.add_argument('--init_sqlfile', type=str, default='')
    args = parser.parse_args()
    IncrSql(**vars(args)).run()

