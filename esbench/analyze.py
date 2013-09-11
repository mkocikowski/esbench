


def results(conn): 
    status, reason, data = conn.get("stats/doc/_count")
    count = json.loads(data)['count']
    for i in range(1, count+1): 
        status, reason, data = conn.get("stats/doc/%i" % i)
        yield json.loads(data)


r = [(d['_source']['docs']['count'],
      d['_source']['search']['mlt']['query_time_in_millis'], 
      d['_source']['search']['match']['query_time_in_millis'], 
      d['_source']['segments']['num_search_segments'], 
      d['_source']['segments']['t_optimize_in_millis'] / 1000.0)
      for d in results(conn)]
print("%8s %7s %7s %4s %12s" % ('COUNT', 'MLT', 'MATCH', 'SEG', 'OPTIMIZE'))
for t in r:
    print("%8i %7i %7i %4i %9.2f" % t)

