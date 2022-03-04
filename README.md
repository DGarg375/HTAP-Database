This is a simplified Hybrid Transactional and Analytical Processing (HTAP) Database based on L-Store.

This is a preliminary build for a single-threaded, in-memory database relational database system which includes capturing the data model, a simple SQL-like query interface, to bufferpool management (managing data in-memory).

The Data Model stores the schema and instance for every table in columnar form. Bufferpool maintains data in-memory. The Query Interface offers data manipulation and querying capability such as select, insert, update, delete of a single key along with a simple aggregation query, namely, to return the summation of a single column for a range of keys.
