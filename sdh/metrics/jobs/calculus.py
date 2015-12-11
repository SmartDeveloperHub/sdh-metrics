"""
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  This file is part of the Smart Developer Hub Project:
    http://www.smartdeveloperhub.org

  Center for Open Middleware
        http://www.centeropenmiddleware.com/
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Copyright (C) 2015 Center for Open Middleware.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at 

            http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
"""

__author__ = 'Fernando Serena'

import calendar
from datetime import date
from threading import Thread
from datetime import datetime
from rdflib import Literal
import multiprocessing
import logging

__calculus = set([])
__dates = {}
__triggers = {}

log = logging.getLogger('sdh.metrics')

workers = multiprocessing.cpu_count()
MAX_ACUM_DATES = workers * 10


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    if n:
        for i in xrange(0, len(l), n):
            yield l[i:i + n]


def add_calculus(func, triggers):
    __calculus.add(func)
    if triggers is not None:
        for trigger in triggers:
            if trigger not in __triggers.keys():
                __triggers[trigger] = set([])
            __triggers[trigger].add(func)


def start_date_calculus(stop_event):
    def get_calculus(_d):
        collectors = __dates[_d]
        return set.union(*[__triggers[c] for c in collectors])

    date_chunks = chunks(list(__dates.keys()), workers)
    for chunk in date_chunks:
        threads = []
        for dt in chunk:
            thread = Thread(target=calculate_metrics,
                            args=(dt, stop_event, get_calculus(dt)))
            threads.append(thread)
            thread.start()
        [t.join() for t in threads]
    __dates.clear()


def check_triggers(collector, quad, stop_event, commit_fn):
    if collector in __triggers:
        _, _, _, o = quad
        if isinstance(o, Literal):
            obj = o.toPython()
            if isinstance(obj, datetime):
                d = date(obj.year, obj.month, obj.day)
                if d not in __dates:
                    __dates[d] = set([])
                __dates[d].add(collector)
                if len(__dates) >= MAX_ACUM_DATES:
                    commit_fn()
                    start_date_calculus(stop_event)


def calculate_metrics(dt, stop_event, calcs):
    calc_date = date(dt.year, dt.month, dt.day)
    pre = datetime.now()
    t_begin = calendar.timegm(calc_date.timetuple())
    end_date = datetime(calc_date.year, calc_date.month, calc_date.day)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    t_end = calendar.timegm(end_date.timetuple())

    # Run all triggered calculus
    for c in calcs:
        try:
            c(t_begin, t_end)
        except Exception, e:
            log.error(e.message)
        if stop_event.isSet():
            break

    took = (datetime.now() - pre).microseconds / float(1000)
    calc_names = [f.func_name for f in calcs]
    log.info('Updated {} calculations ({}) for day {} in {}ms'.format(len(calc_names), calc_names, calc_date, took))
