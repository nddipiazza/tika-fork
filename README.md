# tika-fork

Utility that allows you to run Tika as a forked JVM to minimize memory issues.

It is a common issue when dealing with Tika to have parses that cause your entire JVM to crash due to out-of-memory condition.

This program attempts to deal with this problem, and a couple other things:

* Launches a pool of forked JVMs that are all limited by the amount of memory they can use.
* Uses sockets (not HTTP) to send a stream of your content to the Tika parse, and to receive back metadata and parsed content.
* Uses commons-pool to provide fine-grained control the pool of the forked Tika JVMs.
* Provides a very simple "abortAfterMs" parameter to the parse that will throw a TimeoutException if too much time is taken. This will result in the forked JVM to be aborted. This is useful in the situations where the JVM went into GC hell eating tons of CPU and never returning.