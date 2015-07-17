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

from agora.provider.server.base import AgoraApp, get_accept
import calendar
from datetime import datetime
from agora.provider.server.base import APIError, NotFound
from flask import make_response, url_for
from flask_negotiate import produces
from rdflib.namespace import Namespace, RDF
from rdflib import Graph, URIRef, Literal
from functools import wraps
from sdh.metrics.jobs.calculus import check_triggers

import pkg_resources
try:
    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil
    __path__ = pkgutil.extend_path(__path__, __name__)


METRICS = Namespace('http://www.smartdeveloperhub.org/vocabulary/metrics#')
PLATFORM = Namespace('http://www.smartdeveloperhub.org/vocabulary/platform#')


class MetricsGraph(Graph):
    def __init__(self):
        super(MetricsGraph, self).__init__()
        self.bind('metrics', METRICS)
        self.bind('platform', PLATFORM)

    @staticmethod
    def __decide_serialization_format():
        mimes = get_accept()
        if 'text/turtle' in mimes:
            return 'text/turtle', 'turtle'
        elif 'text/rdf+n3' in mimes:
            return 'text/rdf+n3', 'n3'
        else:
            return 'application/xml', 'xml'

    def serialize(self, destination=None, format="xml",
                  base=None, encoding=None, **args):
        content_type, ex_format = self.__decide_serialization_format()
        return content_type, super(MetricsGraph, self).serialize(destination=destination, format=ex_format,
                                                                 base=base, encoding=encoding, **args)


class MetricsApp(AgoraApp):
    @staticmethod
    def __get_metric_definition_graph(md):
        g = MetricsGraph()
        me = URIRef(url_for('__get_definition', md=md, _external=True))
        g.add((me, RDF.type, METRICS.MetricDefinition))
        g.add((me, PLATFORM.identifier, Literal(md)))
        return g

    @staticmethod
    def __return_graph(g):
        content_type, rdf = g.serialize(format=format)
        response = make_response(rdf)
        response.headers['Content-Type'] = content_type
        return response

    @produces('text/turtle', 'text/rdf+n3', 'application/rdf+xml', 'application/xml')
    def __get_definition(self, md):
        if md not in self.metrics.values():
            raise NotFound('Unknown metric definition')

        g = self.__get_metric_definition_graph(md)
        return self.__return_graph(g)

    @produces('text/turtle', 'text/rdf+n3', 'application/rdf+xml', 'application/xml')
    def __root(self):
        g = MetricsGraph()
        me = URIRef(url_for('__root', _external=True))
        g.add((me, RDF.type, METRICS.MetricService))
        for mf in self.metrics.keys():
            endp = URIRef(url_for(mf, _external=True))
            g.add((me, METRICS.hasEndpoint, endp))

            mident = self.metrics[mf]
            md = URIRef(url_for('__get_definition', md=mident, _external=True))
            g.add((me, METRICS.calculatesMetric, md))
            g.add((md, RDF.type, METRICS.MetricDefinition))
            g.add((md, PLATFORM.identifier, Literal(mident)))

        return self.__return_graph(g)

    def __init__(self, name, config_class):
        super(MetricsApp, self).__init__(name, config_class)

        self.metrics = {}
        self.route('/metrics')(self.__root)
        self.route('/metrics/definitions/<md>')(self.__get_definition)
        self.store = None

    def __metric_rdfizer(self, func):
        g = Graph()
        g.bind('metrics', METRICS)
        g.bind('platform', PLATFORM)
        me = URIRef(url_for(func, _external=True))
        g.add((me, RDF.type, METRICS.MetricEndpoint))
        g.add((me, METRICS.supports, URIRef(url_for('__get_definition', md=self.metrics[func], _external=True))))

        return g

    def __add_context(self, f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            data = f(*args, **kwargs)
            context = kwargs
            context['timestamp'] = calendar.timegm(datetime.utcnow().timetuple())
            if isinstance(data, tuple):
                context.update(data[0])
                data = data[1]
            if type(data) == list:
                context['size'] = len(data)
            return context, data

        return wrapper

    def metric(self, path, handler, mid):
        def decorator(f):
            f = self.__add_context(f)
            f = self.register('/metrics' + path, handler, self.__metric_rdfizer)(f)
            self.metrics[f.func_name] = mid
            return f
        return decorator

    def calculus(self, triggers=None):
        def decorator(f):
            from sdh.metrics.jobs.calculus import add_calculus
            add_calculus(f, triggers)
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
        begin = request.args.get('begin', None)
        if begin is not None:
            begin = int(begin)
        end = request.args.get('end', None)
        if end is not None:
            end = int(end)
        if end is not None and end is not None:
            if end < begin:
                raise APIError('Begin cannot be higher than end')
        return {'begin': begin, 'end': end}

    @staticmethod
    def _get_tbd_context(request):
        begin = int(request.args.get('begin', 0))
        end = int(request.args.get('end', calendar.timegm(datetime.utcnow().timetuple())))
        if end < begin:
            raise APIError('Begin cannot be higher than end')
        return {'begin': begin, 'end': end}

    def _get_metric_context(self, request):
        _max = request.args.get('max', 1)
        context = self._get_basic_context(request)
        context['max'] = max(0, int(_max))
        if context['begin'] is not None and context['end'] is not None:
            context['step'] = context['end'] - context['begin']
        else:
            context['step'] = None
        if context['max'] and context['step'] is not None:
            context['step'] /= context['max']
            if not context['step']:
                raise APIError('Resulting step is 0')

        return context

    def orgmetric(self, path, aggr, mid):
        def context(request):
            return [], self._get_metric_context(request)

        return lambda f: self.metric(path, context, '{}-org-{}'.format(aggr, mid))(f)

    def repometric(self, path, aggr, mid):
        def context(request):
            return [self._get_repo_context(request)], self._get_metric_context(request)

        return lambda f: self.metric(path, context, '{}-repo-{}'.format(aggr, mid))(f)

    def usermetric(self, path, aggr, mid):
        def context(request):
            return [self._get_user_context(request)], self._get_metric_context(request)

        return lambda f: self.metric(path, context, '{}-user-{}'.format(aggr, mid))(f)

    def repousermetric(self, path, aggr, mid):
        def context(request):
            return [self._get_repo_context(request), self._get_user_context(request)], self._get_metric_context(request)

        return lambda f: self.metric(path, context, '{}-repo-user-{}'.format(aggr, mid))(f)

    def orgtbd(self, path, mid):
        def context(request):
            return [], self._get_tbd_context(request)

        return lambda f: self.metric(path, context, 'tbd-org-' + mid)(f)

    def repotbd(self, path, mid):
        def context(request):
            return [self._get_repo_context(request)], self._get_tbd_context(request)

        return lambda f: self.metric(path, context, 'tbd-repo-' + mid)(f)

    def usertbd(self, path, mid):
        def context(request):
            return [self._get_user_context(request)], self._get_tbd_context(request)

        return lambda f: self.metric(path, context, 'tbd-user-' + mid)(f)

    def userrepotbd(self, path, mid):
        def context(request):
            return [self._get_repo_context(request), self._get_user_context(request)], self._get_tbd_context(request)

        return lambda f: self.metric(path, context, 'tbd-repo-user-' + mid)(f)

    def calculate(self, collector, quad, stop_event):
        self.store.execute_pending()
        check_triggers(collector, quad, stop_event)
        self.store.execute_pending()

    def run(self, host=None, port=None, debug=None, **options):
        tasks = options.get('tasks', [])
        tasks.append(self.calculate)
        options['tasks'] = tasks
        super(MetricsApp, self).run(host, port, debug, **options)
