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

from agora.provider.server import AgoraApp
import calendar
from datetime import datetime
from agora.provider.server import APIError

import pkg_resources
try:
    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil
    __path__ = pkgutil.extend_path(__path__, __name__)


class MetricsApp(AgoraApp):
    def __init__(self, name):
        import os
        config = os.environ.get('CONFIG', 'sdh.metrics.server.config.DevelopmentConfig')
        super(MetricsApp, self).__init__(name, config)
        from agora.provider.server import config
        config.update(self.config)

    def metric(self, path, handler, calculus=None):
        def decorator(f):
            from sdh.metrics.jobs.calculus import add_calculus
            if calculus is not None:
                add_calculus(calculus)
            f = self.register(path, handler)(f)
            return f
        return decorator

    @staticmethod
    def _get_repo_context(request):
        rid = request.args.get('rid', None)
        if rid is None:
            raise APIError('A repository ID is required')
        return rid

    @staticmethod
    def _get_user_context(request):
        uid = request.args.get('uid', None)
        if uid is None:
            raise APIError('A user ID is required')
        return uid

    @staticmethod
    def _get_basic_context(request):
        begin = request.args.get('begin', 0)
        end = request.args.get('end', calendar.timegm(datetime.utcnow().timetuple()))
        return {'begin': int(begin), 'end': int(end)}

    def _get_metric_context(self, request):
        num = request.args.get('num', 1)
        context = self._get_basic_context(request)
        context['num'] = max(0, int(num))
        return context

    def orgmetric(self, path, calculus=None):
        def context(request):
            return [], self._get_metric_context(request)
        return lambda f: self.metric(path, context, calculus)(f)

    def repometric(self, path, calculus=None):
        def context(request):
            return [self._get_repo_context(request)], self._get_metric_context(request)
        return lambda f: self.metric(path, context, calculus)(f)

    def usermetric(self, path, calculus=None):
        def context(request):
            return [self._get_user_context(request)], self._get_metric_context(request)
        return lambda f: self.metric(path, context, calculus)(f)

    def userrepometric(self, path, calculus=None):
        def context(request):
            return [self._get_repo_context(request), self._get_user_context(request)], self._get_metric_context(request)
        return lambda f: self.metric(path, context, calculus)(f)

    def orgtbd(self, path, calculus=None):
        def context(request):
            return [], self._get_basic_context(request)
        return lambda f: self.metric(path, context, calculus)(f)

    def repotbd(self, path, calculus=None):
        def context(request):
            return [self._get_repo_context(request)], self._get_basic_context(request)
        return lambda f: self.metric(path, context, calculus)(f)

    def usertbd(self, path, calculus=None):
        def context(request):
            return [self._get_user_context(request)], self._get_basic_context(request)
        return lambda f: self.metric(path, context, calculus)(f)

    def userrepotbd(self, path, calculus=None):
        def context(request):
            return [self._get_repo_context(request), context], self._get_basic_context(request)
        return lambda f: self.metric(path, context, calculus)(f)

    @classmethod
    def calculate(cls):
        from sdh.metrics.jobs.calculus import calculate_metrics
        calculate_metrics()

    def run(self, host=None, port=None, debug=None, **options):
        tasks = options.get('tasks', [])
        tasks.append(MetricsApp.calculate)
        options['tasks'] = tasks
        super(MetricsApp, self).run(host, port, debug, **options)

