# tika-fork
Utility for handling tika in forked JVMs to avoid memory issues in the current JVM.


First run the server:

```
./gradlew -PmainClass=org.apache.tika.fork.TikaMain execute --args "9876 http://unec.edu.az/application/uploads/2014/12/pdf-sample.pdf application/pdf"
```

Then run the client:

```
./gradlew -PmainClass=org.apache.tika.fork.TikaForkMain execute --args "/home/ndipiazza/Downloads/pdf-sample.pdf"
```

