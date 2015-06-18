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

import redis
from agora.provider.jobs.collect import collect as acollect
from datetime import datetime

class FragmentStore(object):
    def __init__(self, redis_host):
        pool = redis.ConnectionPool(host=redis_host, port=6379, db=4)
        self.__r = redis.StrictRedis(connection_pool=pool)
        # self.__r.flushdb()
        self.__pipe = self.__r.pipeline()
        self.__pending_transactions = 0

    def execute(self, f):
        f()
        self.__pending_transactions += 1
        if self.__pending_transactions >= 50:
            try:
                self.__pipe.execute()
                self.__pending_transactions = 0
            except Exception, e:
                print e.message

    def execute_pending(self):
        if self.__pending_transactions:
            self.__pipe.execute()
            self.__pending_transactions = 0

    def update_set(self, key, timestamp, value):
        self.__pipe.zremrangebyscore(key, timestamp, timestamp)
        self.execute(lambda: self.__pipe.zadd(key, timestamp, value))

    def collect(self, tp):
        def wrapper(f):
            return acollect(tp, self)(f)
        return wrapper

    @property
    def db(self):
        return self.__r

    @property
    def pipe(self):
        return self.__pipe

    @property
    def first_date(self):
        import calendar
        return calendar.timegm(datetime.utcnow().timetuple())
