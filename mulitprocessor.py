# -*- coding: utf-8 -*-
import sys
sys.path += ["/usr/local/pt_anteater_paw/", "/usr/local/pt_anteater_paw/celery_task"]
import os
import time
import signal
import multiprocessing
from util.log import create_logger
import functools

class Multiprocessor(object):
    '''
    Usage: make a function which has no return multiprocessing

    process_function: a function object which must have two input parameter: quit_event, logger
    logger_name: log file name
    process_count: how many processes execute, default: 2 * cpu_count
    moniter_childprocess_seconds: how many interval main processor check child processor
    process_function_params_dict: pass some parameters to process_function
    '''
    def __init__(self, process_function, logger_name, process_count=(multiprocessing.cpu_count() * 2), moniter_childprocess_seconds=5, process_function_params_dict=None):
        self.process_count = process_count
        self.moniter_childprocess_seconds = moniter_childprocess_seconds
        self._works = []
        self.quit_event = multiprocessing.Event()
        self.logger = create_logger(logger_name)
        self.process_function = functools.partial(process_function, **process_function_params_dict) if process_function_params_dict else process_function
        signal.signal(signal.SIGTERM, self._quit_worker_process)

    def add_one_work_process(self):
        p = multiprocessing.Process(target=self.process_function, args=(self.quit_event, self.logger))
        p.daemon = True
        p.start()
        self._works.append(p)

    def start_worker_process(self, processes):
        self.logger.info('=== start #%s work processes', self.process_count)
        for i in range(processes):
            self.add_one_work_process()
            time.sleep(2)

    def terminate_worker_process(self):
        for i in self._works:
            i.terminate()

    def _quit_worker_process(self, signum, frame):
        self.logger.info('=== send quit event to all the process')
        self.quit_event.set()

    def check_works_state(self):
        dead_process_count = 0
        alive_works = []
        for i in self._works:
            if not i.is_alive():
                dead_process_count += 1
            else:
                alive_works.append(i)
        # self.logger.debug('#%s work deaded', dead_process_count)
        self._works = alive_works
        for i in range(dead_process_count):
            self.add_one_work_process()

    def run(self):
        self.logger.info('=== start to online content parser, pid:%s', os.getpid())
        self.start_worker_process(processes=self.process_count)
        try:
            while 1:
                if self.quit_event.is_set():
                    break
                self.check_works_state()
                time.sleep(self.moniter_childprocess_seconds)
            for i in self._works:
                i.join()
        except:
            self.logger.error('=== happened error, kill all the process', exc_info=True)
            self.terminate_worker_process()


# def worker(quit_event, logger, process_function):
#     logger.info('start worker,pid:%s', os.getpid())
#     while 1:
#         if quit_event.is_set():
#             logger.info('finish worker, pid:%s', os.getpid())
#             break
#         process_function(logger)
