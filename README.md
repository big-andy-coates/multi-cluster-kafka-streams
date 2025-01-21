# Multi-cluster Kafka Streams example

Demonstrates how a single Kafka Streams instance can be configured to read from one cluster and write to another.

The example covers:
 - Consuming a topic from a source cluster. Could easily be extended to consume multiple topics from the source cluster.
 - Builds a state store, with the changelog stored in the destination cluster. 
 - Restoration of state store on an application restart.

Limitations:

 - internal repartition topics won't work, as they are written to the destination cluster, and the consumer that needs to read them is consuming from the source cluster.
   - workaround: run a separate streams app, configured to only use the source cluster, to do any required repartitioning. 
   - Consume the repartitioned data in the multi-cluster streams app from the source cluster.  
 - EOS will not work, as consumer offsets are stored in source cluster, and hence can not be in a transaction with data written to destination cluster.
 - Global state stores should work, but not tested. The global consumer just needs configuring to point to either the source or destination cluster.