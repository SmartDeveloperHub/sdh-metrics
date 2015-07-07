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
from datetime import datetime, date, timedelta

__calculus = set([])

def add_calculus(func):
    __calculus.add(func)


def calculate_metrics(stop_event):
    from sdh.metrics.store import store
    store.execute_pending()
    try:
        t_ini = store.first_date
    except IndexError:
        print 'ERROR: no first element yet'
        t_ini = None

    if t_ini is not None:
        first_date = datetime.utcfromtimestamp(t_ini)
        next_date = date(first_date.year, first_date.month, first_date.day)

        while next_date <= date.today():
            t_begin = calendar.timegm(next_date.timetuple())
            end_date = datetime(next_date.year, next_date.month, next_date.day)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            t_end = calendar.timegm(end_date.timetuple())

            # Run all registered calculus
            for c in __calculus:
                c(t_begin, t_end)

            next_date = next_date + timedelta(days=1)
            if stop_event.isSet():
                return
    store.execute_pending()
