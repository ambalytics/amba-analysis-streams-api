# amba-analysis-streams-api
REST API for stream analysis platform


### endpoints: 
  - [api](api-analysis.ambalytics.com/api)
  - [docs](api-analysis.ambalytics.com/docs)


-------

The implementation of the API is based on *FastAPI*[^1], a python
high-performance framework that is running in a docker container. There
are four primary REST endpoints: publications, authors, field of study,
and statistics. Further, a system check is available to check if the API
and the system are working as expected. The API is automatically
generating interactive documentation with open API JSON. All responses
are timed; these times are added to the result and stored in the
InfluxDB. The calculated times allow monitoring and identifying slow
queries. Further, all results over 1000 Bytes are being compressed with
GZIP, reducing the network data. The PostgreSQL access is implemented
using SQLAlchemy[^2] and InfluxDB using their respective clients.

The “publications” endpoint allows querying publications, trending
publications, and trending publications for a specific author or field
of study and retrieving a single publication. Trending publications can
be retrieved with offset, order, limit, search, and for a specific
duration to allow them to be ideally queried for a table loading the
data as needed.

Fields of study are similar to authors in aggregating publications.
Their endpoints allow retrieving single elements as well as data suited
for tables. Further access to trending fields of studies and authors are
possible.

The “stats” endpoint allows querying data extending the trending table
data to analyze publications further. It allows access to data from
PostgreSQL and data from the influx specifically for a given duration.
Nearly all stats endpoints offer an option to query data for either a
publication, an author, or a field of study.

The API is adjusted to the database scheme and will not query the
InfluxDB for slow high cardinality queries. Instead, it uses PostgreSQL
with optimized InfluxDB queries and specific buckets to keep the
response times low and responsibility high.

[^1]: <https://fastapi.tiangolo.com/>; accessed 10-November-2021

[^2]: <https://www.sqlalchemy.org/>; accessed 10-November-2021
