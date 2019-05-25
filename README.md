# tika-fork

Utility that allows you to run Tika as a forked JVM to minimize memory issues.

It is a common issue when dealing with Tika to have parses that cause your entire JVM to crash due to out-of-memory conditions.

There are also problems where a Tika parse will not return in sufficient time due to GC hell or some other CPU intense process and will cause issues.

This program attempts to deal with these problems:

* Launches a pool of forked JVMs that are all limited by the amount of memory they can use.
* Uses sockets (not HTTP) to send a stream your document content to the Tika parser, and to receive back a stream of metadata and a stream of the parsed content.
* Uses commons-pool to provide fine-grained control the pool of the forked Tika JVMs.
* Provides a very simple "abortAfterMs" parameter to the parse that will throw a TimeoutException if too much time is taken. This will result in the forked JVM to be aborted. This is useful in the situations where the JVM went into GC hell eating tons of CPU and never returning.