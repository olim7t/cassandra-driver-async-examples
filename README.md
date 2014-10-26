Sample code for my blog post published at http://www.datastax.com/dev/blog/java-driver-async-queries.

The unit tests assume that a Cassandra database is running at
localhost:9042. They will create a keyspace called `async_examples`
containing one table and a few sample rows (see `TestBase.java`).
