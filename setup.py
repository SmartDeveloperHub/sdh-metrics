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

from setuptools import setup, find_packages

setup(
    name="SDH-Metrics",
    version="0.2.10",
    author="Fernando Serena",
    author_email="fernando.serena@centeropenmiddleware.com",
    description="A library for making temporal information services for the SDH project.",
    license="Apache 2",
    keywords=["linked-data", "ontology", "path", "sdh"],
    url="https://github.com/smartdeveloperhub/sdh-metrics",
    download_url="https://github.com/smartdeveloperhub/sdh-metrics/tarball/0.2.2",
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['sdh', 'sdh.metrics'],
    install_requires=['flask', 'Flask_Negotiate', 'redis', 'hiredis', 'rdflib', 'Fragment-Service-Provider', 'pytz',
                      'shortuuid'],
    classifiers=[]
)
