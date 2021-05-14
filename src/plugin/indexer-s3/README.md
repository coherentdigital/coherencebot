indexer-s3 plugin for Nutch 
===========================

**indexer-s3 plugin** is used for sending documents from one or more segments to an S3 bucket. 
The configuration for the index writers is on **conf/index-writers.xml** file, included in the official Nutch distribution and it's as follow:

```xml
<writer id="<writer_id>" class="org.apache.nutch.indexwriter.s3.S3IndexWriter">
  <mapping>
    ...
  </mapping>
  <parameters>
    ...
  </parameters>   
</writer>
```

Each `<writer>` element has two mandatory attributes:

* `<writer_id>` is a unique identification for each configuration. This feature allows Nutch to distinguish each configuration, even when they are for the same index writer. In addition, it allows to have multiple instances for the same index writer, but with different configurations.

* `org.apache.nutch.indexwriter.s3.S3IndexWriter` corresponds to the canonical name of the class that implements the IndexWriter extension point. This value should not be modified for the **indexer-s3 plugin**.

## Mapping

The mapping section is explained [here](https://cwiki.apache.org/confluence/display/NUTCH/IndexWriters#IndexWriters-Mappingsection). The structure of this section is general for all index writers.

## Parameters

Each parameter has the form `<param name="<name>" value="<value>"/>` and the parameters for this index writer are:

Parameter Name | Description | Default value
--|--|--
host | Comma-separated list of hostnames to send documents to using [TransportClient](https://static.javadoc.io/org.elasticsearch/elasticsearch/5.3.0/org/elasticsearch/client/transport/TransportClient.html). Either host and port must be defined. | 
port | The port to connect to using [TransportClient](https://static.javadoc.io/org.elasticsearch/elasticsearch/5.3.0/org/elasticsearch/client/transport/TransportClient.html). | 9300
index | Default bucket to send documents to. | nutch
username | Username for auth credentials | no default
password | Password for auth credentials | no default
max.bulk.docs | Maximum size of the bulk in number of documents. | 250
max.bulk.size | Maximum size of the bulk in bytes. | 2500500
