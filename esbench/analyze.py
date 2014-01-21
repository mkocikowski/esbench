# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

"""Code for retrieving, analyzing, and displaying recorded benchmark data. """

from __future__ import print_function

import itertools
import logging
import json
import collections
import re
import types
import sys
import csv

import tabulate

import esbench


logger = logging.getLogger(__name__)


def _get_benchmarks(conn=None, stats_index_name=esbench.STATS_INDEX_NAME):
    """Call the ES server for raw benchmark records."""

    path = "%s/bench/_search?sort=benchmark_start:asc&size=100" % (stats_index_name, )
    resp = conn.get(path)
    return resp


def _benchmarks(resp, benchmark_ids=None):
    """Process response from _get_benchmarks(), yielding benchmark dicts."""

    if not benchmark_ids:
        benchmark_ids = ['all']

    data = json.loads(resp.data)

    try:

        benchmarks = data['hits']['hits']
        for benchmark_id in benchmark_ids:

            if benchmark_id == 'first':
                yield benchmarks[0]
                continue

            if benchmark_id == 'last':
                yield benchmarks[-1]
                continue

            try:
                yield benchmarks[int(benchmark_id)]
                continue
            except (ValueError, IndexError) as exc:
#                 logger.info(exc)
                pass

            for benchmark in benchmarks:
                if (benchmark_id == 'all') or (benchmark_id == benchmark['_id']):
                    yield benchmark

    except KeyError:
        logger.warning("no benchmarks found", exc_info=False)

    return


def _get_observations(conn=None, benchmark_id=None, stats_index_name=esbench.STATS_INDEX_NAME):
    """Call the ES server for raw observation records."""

    if not benchmark_id:
        raise ValueError("invalid 'benchmark_id'")

    path = "%s/obs/_search?q=meta.benchmark_id:%s&sort=meta.observation_start:asc&size=10000" % (stats_index_name, benchmark_id, )
    resp = conn.get(path)
    return resp


def _observations(resp):
    """Process response from _get_observations(), yielding observation dicts."""

    data = json.loads(resp.data)
    for observation in data['hits']['hits']:
        yield observation


def get_data(conn=None, benchmark_ids=None):
    """Get benchmark and observation data.

    Args:
        conn: instance of esbench.api.Conn
        benchmark_ids: list of str, ids of benchmarks to be included, default
            None. When None, include all benchmarks.

    Yields:
        For each observation associated with included benchmark a dict where
        top level keys are 'benchmark' and 'observation', containing pertinent
        data. Effectively, data is denormalized, with the 'benchmark' element
        being the same for all observations for that benchmark.
    """

    for benchmark in _benchmarks(_get_benchmarks(conn=conn), benchmark_ids=benchmark_ids):
        for observation in _observations(_get_observations(conn=conn, benchmark_id=benchmark['_id'])):
            data = {
                "benchmark": benchmark['_source'],
                "observation": observation['_source'],
            }
#             data[u'benchmark'][u'_id'] = benchmark['_id']
#             data[u'observation'][u'_id'] = observation['_id']
            yield data


def filter_tuples(tuples=None, pattern=".*", key_f=lambda x: x[0]):
    """Return tuples matching provided regex.

    Given a list of tuples and a regex pattern string, apply the regex to
    the specified tuple element, returning only tuples which match.

    Args:
        tuples: list of tuples
        pattern: string, regex pattern defauls to '.*'
        key_f: function for selecting tuple element against which the regex
            will be matched, defaults to the first element of each tuple
            (lambda x: x[0])

    Returns:
        sorted list of tuples where regex matched. The key for sorting is the
        same key used for matching.

    Raises:
        TypeError on invalid input

    """

    if type(tuples) is not list:
        raise TypeError("'tuples' must be a list of tuples")

    compiled = re.compile(pattern, re.IGNORECASE)
    filtered = [t for t in tuples if compiled.match(key_f(t))]
    return sorted(filtered, key=key_f)


def flatten_container(container=None):
    """Flattens a container (dict, list, set).

    Args:
        container: a dict, list, set, tuple, any nested combination of.

    Returns:
        list of tuples (name, value) where name is a flattened, period-joined
        list of dict keys and list indexes identifying the value's original
        location in the source container, for example: "foo.bar.2.baz". The
        'value' is an int, float, str, unicode, bool, or None.

    """

    def _flatten(container=container, prefix=None):

        if type(container) in [str, unicode, int, long, float, bool, types.NoneType]:
            flat.append((prefix, container))

        elif type(container) is dict:
            for key in container:
                _flatten(container=container[key], prefix=("%s.%s" % (prefix, key)) if prefix else key)

        elif type(container) in [list, set, tuple]:
            for n, v in enumerate(container):
                _flatten(container=v, prefix=("%s.%i" % (prefix, n)) if prefix else str(n))

        else:
            raise ValueError("cannot process element: %s" % container)

    flat = list()
    _flatten()

    return flat


def group_observations(data=None, fields=None):

    # returns a list of observations, where each observation is a list of
    # (fieldname, value) tuples
    data_flattened = [flatten_container(d) for d in data]

    # filter out tuples in each observation according to pattern.
    data_filtered = [filter_tuples(tuples=t, pattern=fields) for t in data_flattened]

    # sort observations based on their benchmark_id and observation_sequence_no
    def sort_f(d):
        _d = dict(d)
        return (_d['observation.meta.benchmark_id'], _d['observation.meta.observation_sequence_no'])
    data_sorted = sorted(data_filtered, key=sort_f)

    # group observations into a list of benchmarks, where each benchmark is a
    # list of observations, where each observation is [see comments above]
    groups = [list(benchmark_obs) for benchmark_id, benchmark_obs in itertools.groupby(data_sorted, lambda x: dict(x)['observation.meta.benchmark_id'])]
    # sort benchmark groups on timestamp benchmark started
    groups_sorted = sorted(groups, key=lambda x: dict(x[0])['benchmark.meta.benchmark_start'])

    return groups_sorted


FIELDS = (
    "(?!observation.segments.segments)"
        "("
            "(benchmark.meta.benchmark_start)|"
            "(observation.meta.benchmark_id)|"
            "(observation.meta.observation_id)|"
            "(observation.meta.observation_sequence_no)|"
            "(observation.segments.num_committed_segments)|"
            "(observation.segments.num_search_segments)|"
            "(observation.segments.t_optimize_in_millis)|"
            "(observation.stats.docs.count)|"
            "(observation.stats.store.size_in_bytes)|"
            "(observation.stats.fielddata.memory_size_in_bytes)|"
#             "(observation.stats.search.groups.*query_time_in_millis$)"
            "(observation.stats.search.groups.*query_time_in_millis_per_query$)"
        ")"

)


def output_benchmark(fh=None, fmt=None, observations=None):

    keys = [t[0] for t in observations[0]]
    values = [[t[1] for t in o] for o in observations]

    if fmt == 'tab':
        # shorten the keys a bit
        def _shorten(s):
            s = "".join([c for c in s if c not in "aeiou"])
            return s
        keys = [".".join(k.split(".")[-2:]) for k in keys]
        print(tabulate.tabulate(values, headers=keys), file=fh)

    elif fmt == 'csv':
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(keys)
        writer.writerows(values)

    else:
        raise ValueError("unknown output format: %s" % fmt)


def show_benchmarks(conn=None, benchmark_ids=None, fields=None, fmt=None, fh=None):

    data = list(get_data(conn=conn, benchmark_ids=benchmark_ids))
    benchmarks = group_observations(data=data, fields=fields)

    for b in benchmarks:
        output_benchmark(fh=fh, fmt=fmt, observations=b)


def dump_benchmarks(conn=None, benchmark_ids=None, stats_index_name=esbench.STATS_INDEX_NAME):
    """Dump benchmark data as a sequence of curl calls.

    You can save these calls to a file, and then replay them somewhere else.
    """

    for benchmark in _benchmarks(_get_benchmarks(conn=conn, stats_index_name=stats_index_name), benchmark_ids):
        curl = """curl -XPUT 'http://localhost:9200/%s/bench/%s' -d '%s'""" % (stats_index_name, benchmark['_id'], json.dumps(benchmark['_source']))
        print(curl)
        for o in _observations(_get_observations(conn, benchmark['_id'], stats_index_name=stats_index_name)):
            curl = """curl -XPUT 'http://localhost:9200/%s/obs/%s' -d '%s'""" % (stats_index_name, o['_id'], json.dumps(o['_source']))
            print(curl)
    return


def delete_benchmarks(conn=None, benchmark_ids=None, stats_index_name=esbench.STATS_INDEX_NAME):

    if not benchmark_ids:
        resp = conn.delete(stats_index_name)
        logger.info(resp.curl)

    else:
        for benchmark in _benchmarks(_get_benchmarks(conn, stats_index_name=stats_index_name), benchmark_ids=benchmark_ids):
            for observation in _observations(_get_observations(conn, benchmark_id=benchmark['_id'], stats_index_name=stats_index_name)):
                path = "%s/obs/%s" % (stats_index_name, observation['_id'], )
                resp = conn.delete(path)
                logger.info(resp.curl)
            path = "%s/bench/%s" % (stats_index_name, benchmark['_id'], )
            resp = conn.delete(path)
            logger.info(resp.curl)

    return

