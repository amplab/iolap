# iOLAP

iOLAP is an incremental OLAP query engine that provides a smooth trade-off between query accuracy and
latency, and fulfills a full spectrum of user requirements from approximate but timely query execution to a
more traditional accurate query execution.
iOLAP enables interactive incremental query processing
using a novel mini-batch execution model---given an OLAP query,
iOLAP first randomly partitions the input dataset into smaller sets (mini-batches)
and then incrementally processes through these mini-batches by executing a
delta update query on each mini-batch, where each subsequent delta update
query computes an update based on the output of the previous one. 
