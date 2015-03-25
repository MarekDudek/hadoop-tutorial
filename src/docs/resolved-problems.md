Problems encountered and resolved during development
====================================================

* *Class not found* exception when job executed in cluster

    `job.setJarByClass(Job.class);` has to be called

* *Malformed URL* exception when accessing HDFS

    `URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());` has to be called before but only once