# README #

This Plugin resolves fixes HDFS Data Locality on Kubernetes \
It resolves K8s Network locations for Datanodes as well as HDFS Clients \
This ensures, that clients prefer to consume data from datanodes, which are located on the same kubernetes nodes 

### Setup ###

1. Copy the Jar to Namenode, Namenode-Formatter, Checkpointnode, Datanodes in directory: "/opt/hadoop-<version>/share/hadoop/common/lib" 
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
3. If you wish to set a Cache-Expiry interval other than 5 minutes, you can set a Namenode environment-variable with name ``TOPOLOGY_UPDATE_IN_MIN``
4. The namenode needs a K8s-Serviceaccount with permission to `get` all pods in K8s Cluster

### Test ###
1. Create new HDFS-Client Pod (with DEBUG-log setting)
2. Connect on client Pod and run a ``cat`` (or other read command) on a file stored in HDFS
3. Search for following Log-Message in client logs: 
    ````
   DEBUG DFSClient: Connecting to datanode <datanode-ip>
    ````
4. Check if <datanode-ip> matches IP of the datanode which is located on same kubernetes-node as Client-Pod
5. Repeat this test several times