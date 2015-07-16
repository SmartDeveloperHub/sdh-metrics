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
import types

import pkg_resources

try:
    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil

    __path__ = pkgutil.extend_path(__path__, __name__)


def __build_time_chunk(store, key, begin, end):
    _next = begin
    while _next < end:
        _end = _next + 86400
        stored_values = [eval(res)['v'] for res in store.db.zrangebyscore(key, _next, _end - 1)]
        if stored_values:
            for v in stored_values:
                yield v
        else:
            yield 0
        _next = _end


def aggregate(store, key, begin, end, num, aggr=sum):
    if begin is None:
        end_limit = end
        if end is None:
            end_limit = '+inf'
        _, begin = store.db.zrangebyscore(key, '-inf', end_limit, withscores=True, start=0, num=1).pop()
        data_begin = begin
    else:
        _, data_begin = store.db.zrangebyscore(key, '-inf', '+inf', withscores=True, start=0, num=1).pop()
    if end is None:
        _, end = store.db.zrevrangebyscore(key, '+inf', begin, withscores=True, start=0, num=1).pop()
        data_end = end
    else:
        _, data_end = store.db.zrevrangebyscore(key, '+inf', '-inf', withscores=True, start=0, num=1).pop()

    begin = calendar.timegm(date.fromtimestamp(begin).timetuple())
    end = calendar.timegm(date.fromtimestamp(end).timetuple())

    step_begin = begin
    values = []

    step = end - begin
    if num:
        step /= num
    step = max(86400, step)

    while step_begin <= end - step:
        step_end = step_begin + step
        if aggr == sum and num:
            chunk = [eval(res)['v'] for res in store.db.zrangebyscore(key, step_begin, step_end)]
        else:
            chunk = __build_time_chunk(store, key, step_begin, step_end)
        values.append(chunk)
        step_begin = step_end

    if num:
        result = [aggr(part) for part in values]
    else:
        result = list(values.pop())
        if any(isinstance(el, list) for el in result):
            result = [len(x) for x in result]

    return {'begin': begin, 'end': end, 'data_begin': data_begin, 'data_end': data_end, 'step': step}, result


def avg(x):
    if isinstance(x, types.GeneratorType):
        x = list(x)
    if type(x) == list:
        if x:
            return sum(x) / float(len(x))
    return 0
