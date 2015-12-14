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
from datetime import date, datetime
import types
import math


def flat_sum(lst):
    return sum((flat_sum(elem) if
                hasattr(elem, "__iter__") and not isinstance(elem, basestring)
                else [elem] for elem in lst), [])

def __build_time_chunk(store, key, begin, end, fill):
    _next = begin
    while _next < end:
        _end = _next + 86400
        stored_values = [eval(res)['v'] for res in store.db.zrangebyscore(key, _next, _end - 1)]
        if stored_values:
            for v in stored_values:
                yield v
        else:
            yield fill
        _next = _end


def store_calc(store, key, timestamp, value):
    obj_value = {'t': timestamp, 'v': value}
    store.update_set(key, timestamp, obj_value)


def aggregate(store, key, begin, end, max_n, aggr=sum, fill=0, extend=False):
    def get_step():
        step = end - begin
        step = step / float(max_n) if max_n else 86400
        step = max(86400, step)
        return step

    end_defined = end is not None

    try:
        if begin is None:
            end_limit = end
            if end is None:
                end_limit = '+inf'
            _, begin = store.db.zrangebyscore(key, '-inf', end_limit, withscores=True, start=0, num=1,
                                              score_cast_func=int).pop()
            data_begin = begin
        else:
            _, data_begin = store.db.zrangebyscore(key, '-inf', '+inf', withscores=True, start=0, num=1,
                                                   score_cast_func=int).pop()
        if end is None:
            _, end = store.db.zrevrangebyscore(key, '+inf', begin, withscores=True, start=0, num=1).pop()
            data_end = end
        else:
            _, data_end = store.db.zrevrangebyscore(key, '+inf', '-inf', withscores=True, start=0, num=1).pop()
    except IndexError:
        if begin is None:
            begin = 0
        if end is None:
            end = calendar.timegm(datetime.now().timetuple())
        step = get_step()
        if not max_n:
            max_n = step / 86400
        return {'begin': begin, 'end': end, 'data_begin': None, 'data_end': None, 'step': step}, [0] * max_n

    begin = calendar.timegm(date.fromtimestamp(begin).timetuple())
    end = calendar.timegm(date.fromtimestamp(end).timetuple())

    step_begin = begin
    step_end = begin
    values = []
    step = get_step()

    extend = extend or begin < data_begin or end > data_end or max_n != 1

    if begin == end:
        end = begin + 86400

    if not extend:
        values.append([eval(res)['v'] for res in store.db.zrangebyscore(key, step_begin, step_begin + step)])
    else:
        condition = lambda: (step_begin - end + step) < 0.001 if end_defined else step_begin <= end
        # condition = lambda: step_begin < end
        while condition():
            step_end = step_begin + step
            # step_end = min(end, step_begin + step)
            end_extended = False
            if not end_defined and ((not max_n and step_begin == end) or (max_n and step_end == end)):
                step_end += 0.1
                end_extended = True

            chunk = []
            pre_fill = int(math.ceil((data_begin - step_begin) / 86400))
            if pre_fill > 0:
                chunk += [fill] * pre_fill
            chunk += list(__build_time_chunk(store, key, step_begin, step_end, fill))
            end_overlap = step_end - data_end
            if end_overlap < 0.001:
                end_overlap = 0
            post_fill = int(math.ceil(end_overlap / 86400))
            if post_fill > 0 and not end_extended:
                chunk += [fill] * post_fill

            values.append(chunk)
            step_begin = step_end

    result = [aggr(part) for part in values]
    if not max_n:
        step = 86400

    return {'begin': begin, 'end': end, 'data_begin': data_begin, 'data_end': data_end, 'step': step}, result


def avg(x):
    if isinstance(x, types.GeneratorType):
        x = list(x)
    if type(x) == list:
        if x:
            return sum(x) / float(len(x))
    return 0
