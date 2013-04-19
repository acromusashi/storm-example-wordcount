# Storm WordCount Example Program

storm-example-wordcount contains test examples of using Storm. 

More information about Storm can be found on the [project page](http://github.com/nathanmarz/storm).

## Maven

You can package a jar suitable for submitting to a cluster with this command:

```
mvn clean package
```

This will package your code and all the non-Storm dependencies into a single "uberjar" at the path `target/storm-example-wordcount-{version}-jar-with-dependencies.jar`.

For put messages for kestrel, do below command.

java -cp storm-example-wordcount-1.0.0-SNAPSHOT-jar-with-dependencies.jar storm.starter.MemcachedPutter __KESTREL_HOST__:22133 MessageQueue 1000
