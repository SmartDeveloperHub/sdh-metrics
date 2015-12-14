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
from flask import make_response, url_for, jsonify
from flask_negotiate import produces
from rdflib.namespace import Namespace, RDF
from rdflib import Graph, URIRef, Literal
from functools import wraps
from sdh.metrics.jobs.calculus import check_triggers, start_date_calculus
import shortuuid
import logging
import requests
from urllib import urlencode

log = logging.getLogger('sdh.metrics.server')

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
        # if 'text/turtle' in mimes:
        return 'text/turtle', 'turtle'
        # elif 'text/rdf+n3' in mimes:
        #     return 'text/rdf+n3', 'n3'
        # else:
        #     return 'application/xml', 'xml'

    def serialize(self, destination=None, format="turtle",
                  base=None, encoding=None, **args):
        content_type, ex_format = self.__decide_serialization_format()
        return content_type, super(OperationsGraph, self).serialize(destination=destination, format=ex_format,
                                                                    base=base, encoding=encoding, **args)


def _handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


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
            signature_node = URIRef(me + '#signature')
            g.add((me, PLATFORM.hasSignature, signature_node))
            for i, p in enumerate(parameters):
                g.add((signature_node, RDF.type, PLATFORM.Signature))
                parameter_node = URIRef(me + '#param_{}'.format(i))
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

    # @produces('text/turtle')
    # @produces('text/turtle', 'text/rdf+n3', 'application/rdf+xml', 'application/xml')
    def __get_definition(self, definition):
        if definition not in self.metrics.values() and definition not in self.views.values():
            raise NotFound('Unknown definition')

        g = self.__get_definition_graph(definition, self.parameters.get(definition, None),
                                        self.titles.get(definition, None))
        return self.__return_graph(g)

    # @produces('text/turtle')
    # @produces('text/turtle', 'text/rdf+n3', 'application/rdf+xml', 'application/xml')
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
        self.__available_views = {}
        self.__available_metrics = {}

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
        g.bind('views', VIEWS)
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
            if context['begin'] is None:
                context['begin'] = 0
            if context['end'] is None:
                context['end'] = context['timestamp']
            if 'step' not in context or context['step'] is None:
                context['step'] = context['end'] - context['begin']
            if isinstance(data, tuple):
                context.update(data[0])
                data = data[1]
            if type(data) == list:
                context['size'] = len(data)
            return context, data

        return wrapper

    def _metric(self, path, handler, aggr, id=None, **kwargs):
        def decorator(f):
            mid = id
            if mid is None:
                mid = shortuuid.uuid()
            f = self.__add_context(f)
            f = self.register('/metrics' + path, handler, self.__metric_rdfizer)(f)
            identifier = '{}-{}'.format(aggr, mid)
            self.metrics[f.func_name] = identifier
            if 'parameters' in kwargs:
                self.parameters[identifier] = kwargs['parameters']
            if 'title' in kwargs:
                self.titles[identifier] = kwargs['title']
            return f

        return decorator

    def _view(self, path, handler, target, id=None, **kwargs):
        def decorator(f):
            vid = id
            if vid is None:
                vid = shortuuid.uuid()
            f = self.__add_context(f)
            f = self.register('/views' + path, handler, self.__view_rdfizer)(f)
            self.views[f.func_name] = vid
            self.view_targets[vid] = target
            if 'parameters' in kwargs:
                self.parameters[vid] = kwargs['parameters']
            if 'title' in kwargs:
                self.titles[vid] = kwargs['title']
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
        try:
            begin = int(request.args.get('begin', 0))
            end = int(request.args.get('end', calendar.timegm(datetime.utcnow().timetuple())))
            if end < begin:
                raise APIError('Begin cannot be higher than end')
            return {'begin': begin, 'end': end}
        except ValueError, e:
            raise APIError(e.message)

    def _get_metric_context(self, request):
        _max = request.args.get('max', 1)
        context = self._get_basic_context(request)
        try:
            context['max'] = max(0, int(_max))
            if context['begin'] is not None and context['end'] is not None:
                context['step'] = context['end'] - context['begin']
            else:
                context['step'] = None
            if context['max'] and context['step'] is not None:
                context['step'] /= context['max']
                if not context['step']:
                    raise APIError('Resulting step is 0')
        except ValueError, e:
            raise APIError(e.message)
        return context

    def __context_by_parameter(self, param):
        if param == ORG.Person:
            return self._get_member_context
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
            return map(lambda f: f(request), map(self.__context_by_parameter, parameters)), self._get_metric_context(
                request)

        return lambda f: self._metric(path, context, aggr, **kwargs)(f)

    def view(self, path, target, **kwargs):
        def context(request):
            parameters = kwargs.get('parameters', [])
            return map(lambda x: x(request), map(self.__context_by_parameter, parameters)), self._get_view_context(
                request)

        return lambda f: self._view(path, context, target, **kwargs)(f)

    def calculate(self, collector, quad, stop_event):
        check_triggers(collector, quad, stop_event, self.store.execute_pending)

    def commit(self, stop_event):
        self.store.execute_pending()
        start_date_calculus(stop_event)
        self.store.execute_pending()

    def __request_endpoint(self, endpoint, **kwargs):
        query_params = urlencode({q: kwargs[q] for q in kwargs if kwargs[q] is not None})
        url = '{}?{}'.format(endpoint, query_params)
        log.debug('Requesting external endpoint {}...'.format(url))
        start_req_time = datetime.now()
        response = requests.get(url, headers={'Accept': 'application/json'})
        if response.status_code == 200:
            r_json = response.json()
            log.debug('Received data from {} after {} s'.format(url, (datetime.now() - start_req_time).total_seconds()))
            return r_json['context'], r_json['result']
        raise EnvironmentError(response.text)

    def request_metric(self, mid, **kwargs):
        endpoint = self.__available_metrics.get(mid, None)
        if endpoint is not None:
            return self.__request_endpoint(endpoint, **kwargs)
        raise AttributeError('No endpoint currently available for metric {}'.format(mid))

    def request_view(self, vid, **kwargs):
        endpoint = self.__available_views.get(vid, None)
        if endpoint is not None:
            return self.__request_endpoint(endpoint, **kwargs)
        raise AttributeError('No endpoint currently available for view {}'.format(vid))

    def __seek_available_endpoints(self):
        from sdh.fragments.jobs.query import get_query_generator
        import time

        while True:
            prefixes, gen = get_query_generator('?endpoint metrics:supports ?_md',
                                                '?_md platform:identifier ?id',
                                                wait=True, **self.config['PROVIDER'])

            aux_dict = {}
            for headers, res in gen:
                mid, endpoint = res['id'], res['endpoint']
                aux_dict[res['id']] = res['endpoint']
                log.info('Found metric: {} at {}'.format(mid, endpoint))
            self.__available_metrics = aux_dict.copy()

            prefixes, gen = get_query_generator('?endpoint views:supports ?_vd',
                                                '?_vd platform:identifier ?id',
                                                wait=True, **self.config['PROVIDER'])

            aux_dict.clear()
            for headers, res in gen:
                vid, endpoint = res['id'], res['endpoint']
                aux_dict[res['id']] = res['endpoint']
                log.info('Found view: {} at {}'.format(vid, endpoint))
            self.__available_views = aux_dict.copy()

            time.sleep(30)

    def run(self, host=None, port=None, debug=None, **options):
        triggers = options.get('triggers', [])
        triggers.append(self.calculate)
        options['triggers'] = triggers

        finishers = options.get('finishers', [])
        finishers.append(self.commit)
        options['finishers'] = finishers

        from threading import Thread

        seek_thread = Thread(target=self.__seek_available_endpoints)
        seek_thread.daemon = True
        seek_thread.start()

        super(MetricsApp, self).run(host, port, debug, **options)
