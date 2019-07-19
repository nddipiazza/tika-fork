# tika-fork

Utility that allows you to run Tika as a pool of forked JVMs to minimize memory issues.

## Motiviation

It is a common issue when dealing with Tika to have parses that cause your entire JVM to crash due to out-of-memory conditions. There are some parameters that are intended to prevent these issues but the issues can still happen from time to time as described in https://issues.apache.org/jira/browse/TIKA-2575

There are also problems where a Tika parse will not return in sufficient time due to GC hell or some other CPU intense process and will cause issues.

This program attempts to deal with these problems:

* Launches a pool of forked JVMs that are all limited by the amount of memory they can use.
* Uses sockets (not HTTP) to send a stream of your document content to the Tika parser, and to receive back a stream of metadata and a stream of the parsed content.
* Uses commons-pool to provide fine-grained control the pool of the forked Tika JVMs.
* Returns various parameters to guarantee Tika parsers return a safe number of bytes, within a certain timeout, and will not have too many instances running at any given point.

## Parameters

```
maxHeapMb - the maximum heap size used by a forked tika parser
parseTimeoutMs - The maximum amount of time in milliseconds that a single parse can take. If a parser does not return by this time, the forked tika parser will be terminated.
minIdle - The minimum number of tika processes that must be live in the pool. -1 for no minimum.
maxIdle - The maximum number of tika processes that may be idle in the pool. -1 for no maximum.
maxTotal - The maximum number of tika processes allowed to be active. -1 for no limit.
blockWhenExhausted -Block when pool is exhausted. If this is set, will block the thread and wait until a tika process is available.
maxWaitMillis - Max wait for tika process - The maximum time to wait for a tika process.
timeBetweenEvictionRunsMillis - Time between eviction checks. The amount of time between the Eviction Runner Thread checks for idle tika processes to evict from the pool.
maxBytesReturned - Sets the maximum amount of bytes that can be returned from the parser.
```

## Usage

See the [Tika Fork Process Unit Tests](https://github.com/nddipiazza/tika-fork/tree/master/tika-fork-client/src/test/java/org/apache/tika/fork) for several detailed examples of how to use the program.
