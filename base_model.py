# -*- coding: utf-8 -*-
from collections import defaultdict

import tornado.log
import datetime
import decimal

from util.pt_tornado_logging import PTTornadoLogger
from util.sql_helper import SqlHelper, transaction_context


class BaseModel(object):
    '''
    If execute a few sql in one transaction:
        with transaction_context:
            BaseModel().insert_one_row(val_tuple)
            BaseModel().set_field(set_field_dict).where(where_dict).update()
    Example see test/sql_helper_test.py
    '''

    _TABLE = ''
    _UNIQUE_KEY_LIST = []
    _DATABASE = 'db_python'
    DEFAULT_DATETIME = datetime.datetime(1970, 1, 1)

    def __init__(self, logger=None):
        if logger is None:
            self.logger = PTTornadoLogger()
        else:
            self.logger = logger
        self.db_helper = SqlHelper(logger=logger)
        if self._TABLE == '':
            raise Exception('ModelError: Not Set _TABLE value')
        self._init_sql_and_val_list()

    def __del__(self):
        # self.db_helper.close_conn()
        self.db_helper = None
        # self.logger.debug('BaseModel.__del__ called')

    def _unqiue_key_func(self, row):
        raise NotImplementedError()

    def _init_sql_and_val_list(self):
        self._sql = {'select': '*', 'where': '', 'group_by': '', 'order_by': '', 'limit': '', 'set': '', 'for_update': ''}
        self._val_list = []

    def _build_where(self, where_dict=None):
        if where_dict is None:
            return '', []
        where_str_list = []
        val_list = []
        for k, v in where_dict.items():
            if v is None:
                where_str_list.append('`{0}` is NULL'.format(k))
            if isinstance(v, list):
                where_str_list.append('`{0}` IN ({1})'.format(k, ','.join(['%s'] * len(v))))
                val_list += v
            elif isinstance(v, tuple):
                if v[0] == 'left_like':
                    where_str_list.append("`{0}` LIKE %s".format(k))
                    val_list.append('%{0}'.format(v[1]))
                elif v[0] == 'right_like':
                    where_str_list.append("`{0}` LIKE %s".format(k, v[1]))
                    val_list.append('{0}%'.format(v[1]))
                elif v[0] == 'like':
                    where_str_list.append("`{0}` LIKE %s".format(k, v[1]))
                    val_list.append('%{0}%'.format(v[1]))
                else:
                    where_str_list.append('`{0}` {1} %s'.format(k, v[0]))
                    val_list.append(v[1])
            else:
                where_str_list.append('`{0}`=%s'.format(k))
                val_list.append(v)
        if len(where_str_list) > 0:
            where_str = 'WHERE ' + ' AND '.join(where_str_list)
        else:
            where_str = ''
        return where_str, val_list

    def _assemble_select_sql(self):
        sql = 'SELECT {0} FROM {1} {2} {3} {4} {5} {6}'.format(
            self._sql['select'],
            self._TABLE,
            self._sql['where'],
            self._sql['group_by'],
            self._sql['order_by'],
            self._sql['limit'],
            self._sql['for_update']
        )
        return sql

    def _assemble_update_sql(self):
        sql = 'UPDATE {0} SET {1} {2}'.format(
            self._TABLE,
            self._sql['set'],
            self._sql['where'],
        )
        return sql

    def update_or_insert_one_row(self, field_val_dict, set_updated_at_now_bool=True, set_created_at_now_bool=True):
        unique_field_val_dict = dict([(k, v) for k, v in field_val_dict.items() if k in self._UNIQUE_KEY_LIST])
        if len(unique_field_val_dict) == 0:
            raise Exception('ModelError: field_val_dict do not have unique_key value')

        with transaction_context:
            rs = self.select_field(['pkid']).where(unique_field_val_dict).for_update().get_one()
            if rs and len(rs) > 0:
                self.set_field(field_val_dict, set_updated_at_now_bool=set_updated_at_now_bool).where(rs).update()
                return rs['pkid']
            else:
                return self.insert_one_row(field_val_dict, set_created_at_now_bool=set_created_at_now_bool)

    def find_or_insert_one_row(self, field_val_dict, set_created_at_now_bool=True):
        unique_field_val_dict = dict([(k, v) for k, v in field_val_dict.items() if k in self._UNIQUE_KEY_LIST])
        if len(unique_field_val_dict) == 0:
            raise Exception('ModelError: field_val_dict do not have unique_key value')
        rs = self.select_field(['pkid']).where(unique_field_val_dict).get_one()
        if rs and len(rs) > 0:
            return rs['pkid']
        else:
            return self.insert_one_row(field_val_dict, set_created_at_now_bool=set_created_at_now_bool)

    def insert_one_row(self, field_val_dict, set_created_at_now_bool=True):
        assert isinstance(field_val_dict, dict)
        field_list = field_val_dict.keys()
        placeholder_list = ['%s'] * len(field_list)
        if set_created_at_now_bool:
            field_list.append('created_at')
            placeholder_list.append('NOW()')
        field_str = ','.join(['`{0}`'.format(f) for f in field_list])
        placeholder_str = ','.join(placeholder_list)
        sql = 'INSERT INTO {0}({1}) VALUES({2})'.format(self._TABLE, field_str, placeholder_str)
        val_tuple = tuple(field_val_dict.values())
        self.logger.debug('sql:%s, %s', sql, str(val_tuple))
        affected_row_count_int, insert_id = self.db_helper.insert(sql, val_tuple)
        return insert_id

    def insert_many_row(self, field_val_dict_list, set_created_at_now_bool=True):
        assert isinstance(field_val_dict_list, list)
        field_list = field_val_dict_list[0].keys()
        placeholder_list = ['%s'] * len(field_list)
        if set_created_at_now_bool:
            field_list.append('created_at')
            placeholder_list.append('NOW()')
        field_str = ','.join(['`{0}`'.format(f) for f in field_list])
        placeholder_str = ','.join(placeholder_list)
        sql = 'INSERT INTO {0}({1}) VALUES({2})'.format(self._TABLE, field_str, placeholder_str)
        val_tuple_list = [i.values() for i in field_val_dict_list]
        self.logger.debug('sql:%s, %s', sql, str(val_tuple_list))
        affected_row_count_int = self.db_helper.execute_many(sql, val_tuple_list)
        return affected_row_count_int

    def update_or_insert_one_row_by_compare_fields(self, field_val_dict, parent_pkid_name, set_created_at_now_bool=True):
        '''
        in order compare fields you need to implement _unqiue_key_func method
        :param field_val_dict:
        :param parent_pkid_name:
        :param set_created_at_now_bool:
        :return:
        '''
        assert isinstance(field_val_dict, dict)
        parent_pkid = field_val_dict[parent_pkid_name]
        rows = self.where({parent_pkid_name: parent_pkid}).get_many()
        if len(rows) == 0:
            return self.insert_one_row(field_val_dict, set_created_at_now_bool)
        else:
            original_rows_dict = defaultdict(list)
            for i in rows:
                original_rows_dict[self._unqiue_key_func(i)].append(i)
            fr = original_rows_dict.get(self._unqiue_key_func(field_val_dict))
            if fr:
                self.set_field(field_val_dict).where({'pkid': fr[0]['pkid']}).update()
            else:
                self.insert_one_row(field_val_dict, set_created_at_now_bool)

    def incr_insert_many_row(self, field_val_dict_list, parent_pkid_names, set_created_at_now_bool=True, update_exist_row=False):
        '''
         when set update_exist_row to True, the method will update exist rows with field_val_dict_list
         NOTES: for the two same unqiue_key rows, it will firstly pick one row randomly to update, and then update another row.
        '''
        assert isinstance(field_val_dict_list, list)
        assert parent_pkid_names != ''
        if len(field_val_dict_list) == 0:
            return
        where_dict = {}
        for k in parent_pkid_names.split(','):
            where_dict[k.strip()] = field_val_dict_list[0][k.strip()]
        rows = self.where(where_dict).get_many()
        if len(rows) == 0:
            return self.insert_many_row(field_val_dict_list)
        else:
            original_rows_dict = defaultdict(list)
            for i in rows:
                original_rows_dict[self._unqiue_key_func(i)].append(i)
            add_row_index, updated_exist_rows = [], []
            for j, dict_obj in enumerate(field_val_dict_list):
                fr = original_rows_dict.get(self._unqiue_key_func(dict_obj))
                if fr:
                    exist_row = fr.pop()
                    field_val_dict_list[j].update({'pkid': exist_row['pkid']})
                    updated_exist_rows.append(field_val_dict_list[j])
                else:
                    add_row_index.append(j)
            affected_row_count = 0
            if add_row_index:
                affected_row_count += self.insert_many_row([field_val_dict_list[a] for a in add_row_index], set_created_at_now_bool)
            if update_exist_row and len(updated_exist_rows) > 0:
                affected_row_count += self.update_many_row(updated_exist_rows, ['pkid'])

    def update_many_row(self, field_val_dict_list, where_field_list):
        assert isinstance(field_val_dict_list, list)
        assert isinstance(where_field_list, list)

        set_str_list = []
        for k in field_val_dict_list[0]:
            if k not in where_field_list:
                set_str_list.append('`{0}`=%s'.format(k))

        set_str_list += ['`updated_at`=NOW()']

        # print set_str_list

        where_str = ''
        total_set_val_list = []
        for f_v_dict in field_val_dict_list:
            set_val_list = []
            where_val_dict = {}
            for k, v in f_v_dict.items():
                if k not in where_field_list:
                    set_val_list.append(v)
                else:
                    where_val_dict[k] = v
            if len(where_val_dict) > 0:
                where_str, val_list = self._build_where(where_val_dict)
                set_val_list += val_list
            total_set_val_list.append(tuple(set_val_list))

        sql = 'UPDATE {0} SET {1} {2}'.format(self._TABLE, ','.join(set_str_list), where_str)
        # print sql
        return self.db_helper.execute_many(sql, total_set_val_list)

    def get_one(self, dict_result_bool=True):
        try:
            sql = self._assemble_select_sql()
            self.logger.debug('sql:%s, %s', sql, self._val_list)
            rs = self.db_helper.query_one(sql, tuple(self._val_list), dict_result_bool=dict_result_bool)
            return rs
        finally:
            self._init_sql_and_val_list()

    def get_many(self, dict_result_bool=True):
        try:
            sql = self._assemble_select_sql()
            self.logger.debug('sql:%s, %s', sql, self._val_list)
            rs = self.db_helper.query_all(sql, tuple(self._val_list), dict_result_bool=dict_result_bool)
            return list(rs) if rs and len(rs) > 0 else []
        finally:
            self._init_sql_and_val_list()

    def select_field(self, selected_field_list=None):
        try:
            assert selected_field_list is None or isinstance(selected_field_list, list)
            if selected_field_list is not None:
                self._sql['select'] = ','.join(selected_field_list)
            return self
        except:
            self._init_sql_and_val_list()
            raise

    def where(self, where_dict):
        try:
            assert isinstance(where_dict, dict)
            where_str, val_list = self._build_where(where_dict)
            self._sql['where'] = where_str
            self._val_list += val_list
            return self
        except:
            self._init_sql_and_val_list()
            raise

    def group_by(self, field_list):
        try:
            assert isinstance(field_list, list)
            self._sql['group_by'] = 'GROUP BY ' + ','.join(['`{0}`'.format(f) for f in field_list])
            return self
        except:
            self._init_sql_and_val_list()
            raise

    def order_by(self, field_dict):
        try:
            assert isinstance(field_dict, dict)
            orderby_str_list = []
            for k, v in field_dict.items():
                if v.upper() in ('ASC', 'DESC'):
                    orderby_str_list.append('`{0}` {1}'.format(k, v.upper()))
                else:
                    raise Exception('%s has invalid value %s in order by params' % (k, v))
            orderby_str = 'ORDER BY ' + ','.join(orderby_str_list)
            self._sql['order_by'] = orderby_str
            return self
        except:
            self._init_sql_and_val_list()
            raise

    def limit(self, offset_int, limit_int):
        try:
            assert isinstance(limit_int, int)
            assert isinstance(offset_int, int)
            limit_str = 'limit {0},{1}'.format(offset_int, limit_int)
            self._sql['limit'] = limit_str
            return self
        except:
            self._init_sql_and_val_list()
            raise

    def for_update(self):
        try:
            self._sql['for_update'] = 'FOR UPDATE'
            return self
        except:
            self._init_sql_and_val_list()
            raise

    def set_field(self, set_field_dict=None, set_updated_at_now_bool=True):
        try:
            assert set_field_dict is None or isinstance(set_field_dict, dict)
            set_val_list = []
            set_str_list = []
            if set_field_dict is not None:
                for k, v in set_field_dict.items():
                    set_str_list.append('`{0}`=%s'.format(k))
                    set_val_list.append(v)
            if set_updated_at_now_bool:
                set_str_list.append('`updated_at`=NOW()')
            self._sql['set'] = ','.join(set_str_list)
            self._val_list += set_val_list
            return self
        except:
            self._init_sql_and_val_list()
            raise

    def update(self):
        try:
            sql = self._assemble_update_sql()
            self.logger.debug('sql:%s, %s', sql, self._val_list)
            affected_row_count_int = self.db_helper.execute(sql, tuple(self._val_list))
            return affected_row_count_int
        finally:
            self._init_sql_and_val_list()

    def query_many_row(self, sql, val_tuple, dict_result_bool=True):
        self.logger.debug('sql:%s, %s', sql, val_tuple)
        rs = self.db_helper.query_all(sql, val_tuple=val_tuple, dict_result_bool=dict_result_bool)
        return list(rs) if rs else []

    def query_one_row(self, sql, val_tuple, dict_result_bool=True):
        self.logger.debug('sql:%s, %s', sql, val_tuple)
        rs = self.db_helper.query_one(sql, val_tuple=val_tuple, dict_result_bool=dict_result_bool)
        return rs

    def execute(self, sql, val_tuple):
        self.logger.debug('sql:%s, %s', sql, val_tuple)
        rs = self.db_helper.execute(sql, val_tuple=val_tuple)
        return rs

    def execute_many(self, sql, val_tuple):
        self.logger.debug('sql:%s, %s', sql, val_tuple)
        rs = self.db_helper.query_all(sql, val_tuple=val_tuple)
        return rs


    @staticmethod
    def convert_object_to_string(rows_dict_list):
        for index, row in enumerate(rows_dict_list):
            for column in row:
                if isinstance(row[column], datetime.datetime):
                    rows_dict_list[index][column] = row[column].strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(row[column], datetime.date):
                    rows_dict_list[index][column] = row[column].strftime('%Y-%m-%d')
                elif isinstance(row[column], decimal.Decimal):
                    rows_dict_list[index][column] = str(row[column])
        return rows_dict_list