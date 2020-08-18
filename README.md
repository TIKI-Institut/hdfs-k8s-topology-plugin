# README #

This Plugin for HDFS-Namenode resolves the Kubernetes-Cluster-Node for a given Datanode-Pod-VIP.

### Setup ###

1. Build a "fat-jar"(jar with all dependencies) with maven package
    * Add maven-assembly-plugin to your pom.xml(see hdfs-topology-plugin/pom.xml)
    * Run maven package
2. Add the "fat-jar" in Namenode-Dockerfile and rebuild it
3. Add following tags to hdfs-siteconfig.xml
````xml
<property>
    <name>net.topology.node.switch.mapping.impl</name>
    <value>org.apache.hadoop.net.PodToNodeMappingorg.apache.hadoop.net.PodToNodeMapping</value>
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
4. The namenode need a Serviceaccount with the role to get all pods in the whole k8s cluster

### Test ###
1. Create new Client Pod / Add log4j.properties (with DEBUG settings) in Dockerfile or mount
2. Connect on new Pod and run 
    ````shell script
    export JAVA_TOOL_OPTIONS="-Dlog4j.configuration=file:/app/log4j.properties ${JAVA_TOOL_OPTIONS} #For Debug
    hadoop fs -cat <your_file.txt> 
    ````
3. Analyze Pod-Logs and search for 
    ````
   DEBUG DFSClient: Connecting to datanode <datanode-ip>
    ````
4. Check if <datanode-ip> matches IP of the datanode which is located on same kubernetes-node as Client-Pod
5. Repeat this test several times
 