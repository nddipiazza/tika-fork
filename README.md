# tika-fork

Socket programming code challenge.

This utility for handling tika in forked JVMs to avoid memory issues in the current JVM when parsing files with Apache Tika.

First run the server:

```
./gradlew -PmainClass=org.apache.tika.fork.TikaMain execute --args "9876 http://unec.edu.az/application/uploads/2014/12/pdf-sample.pdf application/pdf"
```

Then run the client:

```
./gradlew -PmainClass=org.apache.tika.fork.TikaForkMain execute --args "/home/ndipiazza/Downloads/pdf-sample.pdf"
```

As is, it will parse the file you send in, and it will `System.out.println` the output. But what we want this to do is to send the resulting Metadata and Body contents to the socket `OutputStream`.

Make the changes necessary to send the `Metadata` and contents of the output stream.
