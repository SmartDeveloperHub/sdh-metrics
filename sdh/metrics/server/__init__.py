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

from sdh.fragments.server.base import FragmentApp, get_accept
import calendar
from datetime import datetime
from sdh.fragments.server.base import APIError, NotFound
from flask import make_response, url_for
from flask_negotiate import produces
from rdflib.namespace import Namespace, RDF
from rdflib import Graph, URIRef, Literal, BNode
from functools import wraps
from sdh.metrics.jobs.calculus import check_triggers
import shortuuid


METRICS = Namespace('http://www.smartdeveloperhub.org/vocabulary/metrics#')
VIEWS = Namespace('http://www.smartdeveloperhub.org/vocabulary/views#')
PLATFORM = Namespace('http://www.smartdeveloperhub.org/vocabulary/platform#')
SCM = Namespace('http://www.smartdeveloperhub.org/vocabulary/scm#')
CI = Namespace('http://www.smartdeveloperhub.org/vocabulary/ci#')
ORG = Namespace('http://www.smartdeveloperhub.org/vocabulary/organization#')


class OperationsGraph(Graph):
    def __init__(self):
        super(OperationsGraph, self).__init__()
        self.bind('metrics', METRICS)
        self.bind('views', VIEWS)
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
        return content_type, super(OperationsGraph, self).serialize(destination=destination, format=ex_format,
                                                                    base=base, encoding=encoding, **args)


class MetricsApp(FragmentApp):
    def __get_definition_graph(self, definition, parameters, title):
        g = OperationsGraph()
        me = URIRef(url_for('__get_definition', definition=definition, _external=True))
        if definition in self.metrics.values():
            g.add((me, RDF.type, METRICS.MetricDefinition))
        else:
            g.add((me, RDF.type, VIEWS.ViewDefinition))
            g.add((me, VIEWS.target, self.view_targets[definition]))
        g.add((me, PLATFORM.identifier, Literal(definition)))
        if parameters:
            signature_node = BNode('signature')
            g.add((me, PLATFORM.hasSignature, signature_node))
            for p in parameters:
                g.add((signature_node, RDF.type, PLATFORM.Signature))
                parameter_node = BNode()
                g.add((signature_node, PLATFORM.hasParameter, parameter_node))
                g.add((parameter_node, RDF.type, PLATFORM.Parameter))
                g.add((parameter_node, PLATFORM.targetType, URIRef(p)))
        if title:
            g.add((me, PLATFORM.title, Literal(title)))
        return g

    @staticmethod
    def __return_graph(g):
        content_type, rdf = g.serialize(format=format)
        response = make_response(rdf)
        response.headers['Content-Type'] = content_type
        return response

    @produces('text/turtle', 'text/rdf+n3', 'application/rdf+xml', 'application/xml')
    def __get_definition(self, definition):
        if definition not in self.metrics.values() and definition not in self.views.values():
            raise NotFound('Unknown definition')

        g = self.__get_definition_graph(definition, self.parameters.get(definition, None),
                                        self.titles.get(definition, None))
        return self.__return_graph(g)

    @produces('text/turtle', 'text/rdf+n3', 'application/rdf+xml', 'application/xml')
    def __root(self):
        g = OperationsGraph()
        me = URIRef(url_for('__root', _external=True))
        if self.metrics:
            g.add((me, RDF.type, METRICS.MetricService))
        for mf in self.metrics.keys():
            endp = URIRef(url_for(mf, _external=True))
            g.add((me, METRICS.hasEndpoint, endp))

            mident = self.metrics[mf]
            md = URIRef(url_for('__get_definition', definition=mident, _external=True))
            g.add((me, METRICS.calculatesMetric, md))
            g.add((md, RDF.type, METRICS.MetricDefinition))
            g.add((md, PLATFORM.identifier, Literal(mident)))
        if self.views:
            g.add((me, RDF.type, METRICS.ViewService))
        for vf in self.views.keys():
            endp = URIRef(url_for(vf, _external=True))
            g.add((me, VIEWS.hasEndpoint, endp))

            vid = self.views[vf]
            vd = URIRef(url_for('__get_definition', definition=vid, _external=True))
            g.add((me, METRICS.producesView, vd))
            g.add((vd, RDF.type, VIEWS.ViewDefinition))
            g.add((vd, PLATFORM.identifier, Literal(vid)))

        return self.__return_graph(g)

    def __init__(self, name, config_class):
        super(MetricsApp, self).__init__(name, config_class)

        self.metrics = {}
        self.views = {}
        self.parameters = {}
        self.view_targets = {}
        self.titles = {}
        self.route('/api')(self.__root)
        self.route('/api/definitions/<definition>')(self.__get_definition)
        self.store = None

    def __metric_rdfizer(self, func):
        g = Graph()
        g.bind('metrics', METRICS)
        g.bind('platform', PLATFORM)
        me = URIRef(url_for(func, _external=True))
        g.add((me, RDF.type, METRICS.MetricEndpoint))
        g.add(
            (me, METRICS.supports, URIRef(url_for('__get_definition', definition=self.metrics[func], _external=True))))

        return g

    def __view_rdfizer(self, func):
        g = Graph()
        g.bind('views', METRICS)
        g.bind('platform', PLATFORM)
        me = URIRef(url_for(func, _external=True))
        g.add((me, RDF.type, VIEWS.ViewEndpoint))
        g.add((me, VIEWS.supports, URIRef(url_for('__get_definition', definition=self.views[func], _external=True))))

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

    def _metric(self, path, handler, aggr, **kwargs):
        def decorator(f):
            f = self.__add_context(f)
            f = self.register('/metrics' + path, handler, self.__metric_rdfizer)(f)
            uuid = '{}-{}'.format(aggr, shortuuid.uuid())
            self.metrics[f.func_name] = uuid
            if 'parameters' in kwargs:
                self.parameters[uuid] = kwargs['parameters']
            if 'title' in kwargs:
                self.titles[uuid] = kwargs['title']
            return f

        return decorator

    def _view(self, path, handler, target, **kwargs):
        def decorator(f):
            f = self.__add_context(f)
            f = self.register('/views' + path, handler, self.__view_rdfizer)(f)
            uuid = shortuuid.uuid()
            self.views[f.func_name] = uuid
            self.view_targets[uuid] = target
            if 'parameters' in kwargs:
                self.parameters[uuid] = kwargs['parameters']
            if 'title' in kwargs:
                self.titles[uuid] = kwargs['title']
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
    def _get_product_context(request):
        prid = request.args.get('prid', None)
        if prid is None:
            raise APIError('A product ID is required')
        return prid

    @staticmethod
    def _get_project_context(request):
        pjid = request.args.get('pjid', None)
        if pjid is None:
            raise APIError('A project ID is required')
        return pjid

    @staticmethod
    def _get_member_context(request):
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
    def _get_view_context(request):
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

    def __context_by_parameter(self, param):
        if param == ORG.Person:
            return self._get_repo_context
        elif param == ORG.Product:
            return self._get_product_context
        elif param == ORG.Project:
            return self._get_project_context
        elif param == SCM.Repository:
            return self._get_repo_context
        else:
            return None

    def metric(self, path, aggr='sum', **kwargs):
        def context(request):
            parameters = kwargs.get('parameters', [])
            return map(self.__context_by_parameter, parameters), self._get_metric_context(request)

        return lambda f: self._metric(path, context, aggr, **kwargs)(f)

    def view(self, path, target, **kwargs):
        def context(request):
            parameters = kwargs.get('parameters', [])
            return map(self.__context_by_parameter, parameters), self._get_view_context(request)

        return lambda f: self._view(path, context, target, **kwargs)(f)

    def calculate(self, collector, quad, stop_event):
        self.store.execute_pending()
        check_triggers(collector, quad, stop_event)
        self.store.execute_pending()

    def run(self, host=None, port=None, debug=None, **options):
        tasks = options.get('tasks', [])
        tasks.append(self.calculate)
        options['tasks'] = tasks
        super(MetricsApp, self).run(host, port, debug, **options)
