# tika-fork

Utility that allows you to run Tika as a pool of forked JVMs to minimize memory issues.

## Motiviation

It is a common issue when dealing with Tika to have parses that cause your entire JVM to crash due to out-of-memory conditions. There are some parameters that are intended to prevent these issues but the issues can still happen from time to time as described in https://issues.apache.org/jira/browse/TIKA-2575

There are also problems where a Tika parse will not return in sufficient time due to GC hell or some other CPU intense process and will cause issues.

This program attempts to deal with these problems:

* Launches a pool of forked JVMs that are all limited by the amount of memory they can use.
* Uses sockets (not HTTP) to send a stream of your document content to the Tika parser, and to receive back a stream of metadata and a stream of the parsed content.
* Uses commons-pool to provide fine-grained control the pool of the forked Tika JVMs.
* Provides a "abortAfterMs" parameter to the parse method that will throw a TimeoutException if too much time is taken. This will result in the forked JVM to be aborted. This is useful in the situations where the JVM went into GC hell eating tons of CPU and never returning.

## Usage

See the [Tika Fork Process Unit Tests](tika-fork/tree/master/tika-fork-client/src/test/java/org/apache/tika/fork) for several detailed examples of how to use the program.
