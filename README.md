# README #

## Description ##
This Plugin fixes HDFS Data Locality on Kubernetes. 
It resolves K8s-Network-Locations for Datanodes as well as HDFS Clients. \
This ensures, that clients prefer to consume data from Datanodes, which are located on same kubernetes nodes.  
Whenever a Datanode-Pod is created, it registeres itself at the Namenode-Pod on which it is assigned to. \
Namenode tries then to map the Datanode to a "Host" and a "Rack" and to create a network-location-String like "/rack/host/datanode". \
When running HDFS on Kuberenetes, this will fail because the hosts of the Datanode-Pods are in fact Kubernetes-Nodes and the native hadoop-resolve methods are not compatible with K8s. \
Namenode will execute the same process, whenever a Client send a request to HDFS.
For details on the underlying algorithm, please read the Javadoc.

## Setup ##

1. Copy the Jar to Namenode-, Namenode-Formatter-, Checkpointnode-, Datanodes-Image in directory: "/opt/hadoop-<version>/share/hadoop/common/lib" 
2. Add following tags to hdfs-siteconfig.xml
    ````xml
    <property>
        <name>net.topology.node.switch.mapping.impl</name>
        <value>org.apache.hadoop.net.PodToNodeMapping</value>
    </property>
    <property>
        <name>net.topology.impl</name>
        <value>org.apache.hadoop.net.NetworkTopologyWithNodeGroup</value>
    </property>
    <property>
        <name>net.topology.nodegroup.aware</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.block.replicator.classname</name>
        <value>org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithNodeGroup</value>
    </property>
    ````
3. If you wish to set a Cache-Expiry-Interval(see Javadoc) other than 5 minutes, you can set a Namenode environment-variable with name ``TOPOLOGY_UPDATE_IN_MIN``
4. The namenode needs a K8s-Serviceaccount with permission to `get` all pods in K8s Cluster

## Test ##
1. Create new HDFS-Client Pod (with DEBUG-log setting)
2. Connect on client Pod and run a ``cat`` (or other read command) on a file stored in HDFS
3. Search for following Log-Message in client logs: 
    ````
   DEBUG DFSClient: Connecting to datanode <datanode-ip>
    ````
4. Check if <datanode-ip> matches IP of the datanode which is located on same kubernetes-node as Client-Pod
5. Repeat this test several times