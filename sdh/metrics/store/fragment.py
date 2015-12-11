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
from sdh.fragments.jobs.collect import collect as acollect
from sdh.fragments.jobs.query import query as aquery
from datetime import datetime
from threading import Lock

class FragmentStore(object):
    def __init__(self, host='localhost', db=5, port=6379, max_pending=200):
        self.__pool = redis.ConnectionPool(host=host, port=port, db=db)
        self.__r = redis.StrictRedis(connection_pool=self.__pool)
        self.__pipe = self.__r.pipeline()
        self.__pending_transactions = 0
        self.__lock = Lock()
        self.__pending_actions = []
        self.__max_pending = max_pending

    def __pipeline_actions(self):
        with self.__r.pipeline(transaction=True) as pipe:
            [pipe.__getattribute__(action_name)(*args) for (action_name, args) in self.__pending_actions]
            try:
                pipe.execute()
                self.__pending_actions = []
            except Exception, e:
                print e.message

    def execute(self, action_name, *args):
        self.__lock.acquire()
        self.__pending_actions.append((action_name, args))
        if len(self.__pending_actions) >= self.__max_pending:
            self.__pipeline_actions()
        self.__lock.release()

    def execute_pending(self):
        self.__lock.acquire()
        if self.__pending_actions:
            self.__pipeline_actions()
        self.__lock.release()

    def update_set(self, key, timestamp, value):
        self.execute('zremrangebyscore', key, timestamp, timestamp)
        self.execute('zadd', key, timestamp, value)

    def collect(self, tp):
        def wrapper(f):
            return acollect(tp, self)(f)
        return wrapper

    def query(self, gp):
        def wrapper(f):
            return aquery(gp, self)(f)
        return wrapper

    @property
    def first_date(self):
        import calendar
        return calendar.timegm(datetime.utcnow().timetuple())

    @property
    def db(self):
        return self.__r
