# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import datetime
import os.path
import unittest
import json
import itertools
import logging

import esbench.bench
import esbench.api
import esbench.client
import esbench.test.test_api


logger = logging.getLogger(__name__)

class BenchTest(unittest.TestCase):

    def test_rands(self):
        s = esbench.bench.rands()
        self.assertIsInstance(s, str)
        self.assertEqual(len(s), 6)
        self.assertNotEqual(esbench.bench.rands(), esbench.bench.rands())



class SearchQueryTest(unittest.TestCase):

    def test_execute(self):

        with self.assertRaises(ValueError):
            q = esbench.bench.SearchQuery(
                    name='match',
                    query=json.dumps({'match': {'foo': 'bar'}}),
                    observation_id='ABCDEFGH',
                    index='test',
                    doctype='doc'
            )

        q = esbench.bench.SearchQuery(
                name='match',
                query={'match': {'foo': 'bar'}},
                observation_id='ABCDEFGH',
                index='test',
                doctype='doc'
        )
        c = esbench.api.Conn(conn_cls=esbench.test.test_api.MockHTTPConnection)
        resp = q.execute(c)
        self.assertEqual(resp.curl, """curl -XPOST http://localhost:9200/test/doc/_search -d \'{"match": {"foo": "bar"}, "stats": ["ABCDEFGH_match"]}\'""")
        self.assertEqual(1, q.execution_count)


class ObservationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.queries = json.loads("""{"mlt": {"fields": ["description.txt"], "from": 0, "query": {"more_like_this": {"fields": ["description.txt"], "like_text": "USING V%(variable)s PROTECTING THE CARTRIDGE FROM WELDING HEAT", "max_query_terms": 25, "min_term_freq": 2, "percent_terms_to_match": 0.3}}, "size": 10}, "match": {"fields": ["description.txt"], "from": 0, "query": {"match": {"description.txt": "computing V%(variable)s device portable"}}, "size": 10}}""")


    def setUp(self):
        self.conn = esbench.api.Conn(conn_cls=esbench.test.test_api.MockHTTPConnection)
        self.observation = esbench.bench.Observation(
                        conn = self.conn,
                        stats_index_name = 'stats',
                        benchmark_id = 'bench1',
                        queries = self.queries,
                        reps = 10,
                        doc_index_name = 'test',
                        doctype = 'doc',
        )


    def test_init(self):
        self.assertIsNone(self.observation.ts_start)
        self.assertIsNone(self.observation.ts_stop)
        self.assertIsInstance(self.observation.queries[1], esbench.bench.SearchQuery)


    def test_segments(self):

        def _f(conn, index):
            return esbench.api.ApiResponse(200, 'ok', """{"ok":true,"_shards":{"total":1,"successful":1,"failed":0},"indices":{"test":{"shards":{"0":[{"routing":{"state":"STARTED","primary":true,"node":"YFJaFqa6Q-m-FPY_IRQ5nw"},"num_committed_segments":3,"num_search_segments":3,"segments":{"_a":{"generation":10,"num_docs":80,"deleted_docs":0,"size":"2.4mb","size_in_bytes":2524210,"committed":true,"search":true,"version":"4.4","compound":false},"_b":{"generation":11,"num_docs":10,"deleted_docs":0,"size":"271.7kb","size_in_bytes":278301,"committed":true,"search":true,"version":"4.4","compound":true},"_c":{"generation":12,"num_docs":10,"deleted_docs":0,"size":"225.3kb","size_in_bytes":230761,"committed":true,"search":true,"version":"4.4","compound":true}}}]}}}}""", "")

        # default behavior is to not return detailed segment info
        s = self.observation._segments(segments_f=_f)
        self.assertIsNone(s['segments'])

        # no instantiate observation with 'record_segment_stats' set to True
        self.observation = esbench.bench.Observation(
                        conn = self.conn,
                        stats_index_name = 'stats',
                        benchmark_id = 'bench1',
                        queries = self.queries,
                        reps = 10,
                        doc_index_name = 'test',
                        doctype = 'doc',
                        record_segment_stats = True,
        )
        s = self.observation._segments(segments_f=_f)
        self.assertEqual(sorted(['_a', '_b', '_c']), sorted(s.pop('segments').keys()))

        # aggregate segment info
        self.assertEqual(s, {'num_search_segments': 3, 't_optimize': None, 't_optimize_in_millis': None, 'num_committed_segments': 3})


    def test_stats(self):

        def _f(conn, index, stats_group_names):
            self.assertEqual(stats_group_names, ",".join([q.stats_group_name for q in self.observation.queries]))
            return esbench.api.ApiResponse(200, 'ok', """{"ok":true,"_shards":{"total":1,"successful":1,"failed":0},"_all":{"primaries":{"docs":{"count":100,"deleted":0},"store":{"size":"2.8mb","size_in_bytes":3024230,"throttle_time":"99.9ms","throttle_time_in_millis":99},"indexing":{"index_total":100,"index_time":"781ms","index_time_in_millis":781,"index_current":0,"delete_total":0,"delete_time":"0s","delete_time_in_millis":0,"delete_current":0},"search":{"open_contexts":0,"query_total":4000,"query_time":"2.2s","query_time_in_millis":2218,"query_current":0,"fetch_total":4000,"fetch_time":"11.1s","fetch_time_in_millis":11108,"fetch_current":0,"groups":{"7140aefb_match_abs":{"query_total":100,"query_time":"28ms","query_time_in_millis":28,"query_current":0,"fetch_total":100,"fetch_time":"91ms","fetch_time_in_millis":91,"fetch_current":0},"7140aefb_mlt":{"query_total":100,"query_time":"11ms","query_time_in_millis":11,"query_current":0,"fetch_total":100,"fetch_time":"1ms","fetch_time_in_millis":1,"fetch_current":0},"7140aefb_match":{"query_total":100,"query_time":"19ms","query_time_in_millis":19,"query_current":0,"fetch_total":100,"fetch_time":"142ms","fetch_time_in_millis":142,"fetch_current":0},"7140aefb_match_srt":{"query_total":100,"query_time":"22ms","query_time_in_millis":22,"query_current":0,"fetch_total":100,"fetch_time":"137ms","fetch_time_in_millis":137,"fetch_current":0}}},"merges":{"current":0,"current_docs":0,"current_size":"0b","current_size_in_bytes":0,"total":1,"total_time":"310ms","total_time_in_millis":310,"total_docs":84,"total_size":"2.8mb","total_size_in_bytes":2946979}},"total":{"docs":{"count":100,"deleted":0},"store":{"size":"2.8mb","size_in_bytes":3024230,"throttle_time":"99.9ms","throttle_time_in_millis":99},"indexing":{"index_total":100,"index_time":"781ms","index_time_in_millis":781,"index_current":0,"delete_total":0,"delete_time":"0s","delete_time_in_millis":0,"delete_current":0},"search":{"open_contexts":0,"query_total":4000,"query_time":"2.2s","query_time_in_millis":2218,"query_current":0,"fetch_total":4000,"fetch_time":"11.1s","fetch_time_in_millis":11108,"fetch_current":0,"groups":{"7140aefb_match_abs":{"query_total":100,"query_time":"28ms","query_time_in_millis":28,"query_current":0,"fetch_total":100,"fetch_time":"91ms","fetch_time_in_millis":91,"fetch_current":0},"7140aefb_mlt":{"query_total":100,"query_time":"11ms","query_time_in_millis":11,"query_current":0,"fetch_total":100,"fetch_time":"1ms","fetch_time_in_millis":1,"fetch_current":0},"7140aefb_match":{"query_total":100,"query_time":"19ms","query_time_in_millis":19,"query_current":0,"fetch_total":100,"fetch_time":"142ms","fetch_time_in_millis":142,"fetch_current":0},"7140aefb_match_srt":{"query_total":100,"query_time":"22ms","query_time_in_millis":22,"query_current":0,"fetch_total":100,"fetch_time":"137ms","fetch_time_in_millis":137,"fetch_current":0}}},"merges":{"current":0,"current_docs":0,"current_size":"0b","current_size_in_bytes":0,"total":1,"total_time":"310ms","total_time_in_millis":310,"total_docs":84,"total_size":"2.8mb","total_size_in_bytes":2946979}}},"indices":{"test":{"primaries":{"docs":{"count":100,"deleted":0},"store":{"size":"2.8mb","size_in_bytes":3024230,"throttle_time":"99.9ms","throttle_time_in_millis":99},"indexing":{"index_total":100,"index_time":"781ms","index_time_in_millis":781,"index_current":0,"delete_total":0,"delete_time":"0s","delete_time_in_millis":0,"delete_current":0},"search":{"open_contexts":0,"query_total":4000,"query_time":"2.2s","query_time_in_millis":2218,"query_current":0,"fetch_total":4000,"fetch_time":"11.1s","fetch_time_in_millis":11108,"fetch_current":0,"groups":{"7140aefb_match_abs":{"query_total":100,"query_time":"28ms","query_time_in_millis":28,"query_current":0,"fetch_total":100,"fetch_time":"91ms","fetch_time_in_millis":91,"fetch_current":0},"7140aefb_mlt":{"query_total":100,"query_time":"11ms","query_time_in_millis":11,"query_current":0,"fetch_total":100,"fetch_time":"1ms","fetch_time_in_millis":1,"fetch_current":0},"7140aefb_match":{"query_total":100,"query_time":"19ms","query_time_in_millis":19,"query_current":0,"fetch_total":100,"fetch_time":"142ms","fetch_time_in_millis":142,"fetch_current":0},"7140aefb_match_srt":{"query_total":100,"query_time":"22ms","query_time_in_millis":22,"query_current":0,"fetch_total":100,"fetch_time":"137ms","fetch_time_in_millis":137,"fetch_current":0}}},"merges":{"current":0,"current_docs":0,"current_size":"0b","current_size_in_bytes":0,"total":1,"total_time":"310ms","total_time_in_millis":310,"total_docs":84,"total_size":"2.8mb","total_size_in_bytes":2946979}},"total":{"docs":{"count":100,"deleted":0},"store":{"size":"2.8mb","size_in_bytes":3024230,"throttle_time":"99.9ms","throttle_time_in_millis":99},"indexing":{"index_total":100,"index_time":"781ms","index_time_in_millis":781,"index_current":0,"delete_total":0,"delete_time":"0s","delete_time_in_millis":0,"delete_current":0},"search":{"open_contexts":0,"query_total":4000,"query_time":"2.2s","query_time_in_millis":2218,"query_current":0,"fetch_total":4000,"fetch_time":"11.1s","fetch_time_in_millis":11108,"fetch_current":0,"groups":{"7140aefb_match_abs":{"query_total":100,"query_time":"28ms","query_time_in_millis":28,"query_current":0,"fetch_total":100,"fetch_time":"91ms","fetch_time_in_millis":91,"fetch_current":0},"7140aefb_mlt":{"query_total":100,"query_time":"11ms","query_time_in_millis":11,"query_current":0,"fetch_total":100,"fetch_time":"1ms","fetch_time_in_millis":1,"fetch_current":0},"7140aefb_match":{"query_total":100,"query_time":"19ms","query_time_in_millis":19,"query_current":0,"fetch_total":100,"fetch_time":"142ms","fetch_time_in_millis":142,"fetch_current":0},"7140aefb_match_srt":{"query_total":100,"query_time":"22ms","query_time_in_millis":22,"query_current":0,"fetch_total":100,"fetch_time":"137ms","fetch_time_in_millis":137,"fetch_current":0}}},"merges":{"current":0,"current_docs":0,"current_size":"0b","current_size_in_bytes":0,"total":1,"total_time":"310ms","total_time_in_millis":310,"total_docs":84,"total_size":"2.8mb","total_size_in_bytes":2946979}}}}}""", "")

        s = self.observation._stats(stats_f=_f)
        self.assertEqual(s['docs']['count'], 100)
        self.assertIsNone(s['search']['groups']['mlt']['client_time'])
        self.assertEqual(0, s['search']['groups']['mlt']['client_total'])
        self.assertEqual(s['store']['size_in_bytes'], 3024230)


    def test_run(self):
        self.observation.run()
        self.assertIsNotNone(self.observation.ts_start)
        self.assertIsNotNone(self.observation.ts_stop)
        self.assertEqual(20, len(self.conn.conn.requests))
        self.assertEqual(10, self.observation.queries[0].execution_count)
        q = json.loads(self.conn.conn.req[2])
        self.assertEqual(
            self.observation.queries[1].stats_group_name,
            json.loads(self.conn.conn.req[2])['stats'][0])


    def test_record(self):
        self.observation.run()
        self.observation._stats = lambda: {}
        self.observation._segments = lambda: {}
        resp = self.observation.record()
        data = json.loads(resp.data)
        self.assertEqual(set(['cluster', 'segments', 'meta', 'stats']), set(data.keys()))
        self.assertEqual(data['meta']['benchmark_id'], self.observation.benchmark_id)


class MockObservation(object):

    def __init__(self, *args, **kwargs):
        self.did_run = False
        self.did_record = False
        self.__dict__.update(kwargs)
#         logger.debug(self.__dict__)

    def run(self):
        self.did_run = True

    def record(self):
        self.did_record = True


class BenchmarkTest(unittest.TestCase):

    def setUp(self):
        self.conn = esbench.api.Conn(conn_cls=esbench.test.test_api.MockHTTPConnection)
        self.cmnd = "run"
        self.argv = esbench.client.args_parser().parse_args(self.cmnd.split())
        self.bench = esbench.bench.Benchmark(self.cmnd, self.argv, self.conn)


    def test_init(self):
        pass


    def test_prepare(self):
        self.assertIsNone(self.bench.ts_start)
        self.assertIsNone(self.bench.t1)
        self.bench.prepare()
        self.assertIsNotNone(self.bench.ts_start)
        self.assertIsNotNone(self.bench.t1)


    def test_load(self):
        lines = ("line_%02i" % i for i in range(12))
        counts = [self.bench.load(itertools.islice(lines, 10)) for _ in range(3)]
        self.assertEqual(counts, [(10, 70), (2, 14), (0, 0)])


    def test_run(self):

        self.obs_count = 0

        def _obs():
            self.obs_count += 1

        batches = esbench.data.batches_iterator(("line_%02i" % i for i in range(100)), batch_count=10, max_n=100, max_byte_size=0)
        self.bench.observe = _obs
        self.bench.run(batches)
        self.assertEqual(len(self.conn.conn.requests), 103)
        self.assertEqual(self.conn.conn.requests[:4], [('PUT', None, '{"settings": {"index": {"number_of_replicas": 0, "number_of_shards": 1}}}'), ('DELETE', u'esbench_test', None), ('PUT', u'esbench_test', '{"mappings": {"doc": {"_size": {"enabled": true, "store": "yes"}, "properties": {"abstract": {"type": "string", "store": "yes"}}, "_source": {"enabled": true}}}, "settings": {"index": {"number_of_replicas": 0, "number_of_shards": 1}}}'), ('POST', u'esbench_test/doc', 'line_00')])
        self.assertEqual(self.obs_count, 10)

        self.obs_count = 0
        self.conn = esbench.api.Conn(conn_cls=esbench.test.test_api.MockHTTPConnection)
        self.cmnd = "run --append"
        self.argv = esbench.client.args_parser().parse_args(self.cmnd.split())
        self.bench = esbench.bench.Benchmark(self.cmnd, self.argv, self.conn)
        batches = esbench.data.batches_iterator(("line_%02i" % i for i in range(100)), batch_count=10, max_n=100, max_byte_size=0)
        self.bench.observe = _obs
        self.bench.run(batches)
        self.assertEqual(len(self.conn.conn.requests), 101)
        self.assertEqual(self.conn.conn.requests[:4], [('PUT', None, '{"settings": {"index": {"number_of_replicas": 0, "number_of_shards": 1}}}'), ('POST', u'esbench_test/doc', 'line_00'), ('POST', u'esbench_test/doc', 'line_01'), ('POST', u'esbench_test/doc', 'line_02')])
        self.assertEqual(self.obs_count, 10)

        self.obs_count = 0
        self.conn = esbench.api.Conn(conn_cls=esbench.test.test_api.MockHTTPConnection)
        self.cmnd = "run --append --observations 5"
        self.argv = esbench.client.args_parser().parse_args(self.cmnd.split())
        self.bench = esbench.bench.Benchmark(self.cmnd, self.argv, self.conn)
        batches = esbench.data.batches_iterator(("line_%02i" % i for i in range(100)), batch_count=5, max_n=100, max_byte_size=0)
        self.bench.observe = _obs
        self.bench.run(batches)
        self.assertEqual(len(self.conn.conn.requests), 101)
        self.assertEqual(self.conn.conn.requests[:4], [('PUT', None, '{"settings": {"index": {"number_of_replicas": 0, "number_of_shards": 1}}}'), ('POST', u'esbench_test/doc', 'line_00'), ('POST', u'esbench_test/doc', 'line_01'), ('POST', u'esbench_test/doc', 'line_02')])
        self.assertEqual(self.obs_count, 5)


    def test_observe(self):
        obs = self.bench.observe(obs_cls=MockObservation)
        self.assertFalse(obs.record_segment_stats) # make sure record_segment_stats is initialized as False by bench.observe() by default
        self.assertEqual(self.conn.conn.requests, [('POST', u'esbench_test/_optimize?refresh=true&flush=true&wait_for_merge=true', None)])
        self.assertTrue(obs.did_run)
        self.assertTrue(obs.did_record)


    def test_record(self):
        # must call .prepare() first
        self.assertRaises(TypeError, self.bench.record)
        self.bench.prepare()
        self.bench._get_cluster_info = lambda: {'foo': 'bar'}
        resp = self.bench.record()
        data = json.loads(resp.data)
        self.assertEqual(set(['meta', 'cluster']), set(data.keys()))
        self.assertEqual(data['meta']['argv']['maxsize'], '1mb')
        self.assertEqual(data['cluster']['foo'], 'bar')
        # TODO: more tests?


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

