package org.apache.hadoop.net;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

/*HDSF-Topology Plugin, which maps Pods to K8s-CusterNodes, on which they are running on
*
* The Method "resolve" gets called by CachedDNSToSwitchMapping and recieves as Input either a DNS-String, in case of datanodes (eg. k8s-worker-1.org.local),
* or a Pod-IP-Address (eg. 10.100.2.3)
*
* With additional Information about on which K8s-Node the datanode and client-pods are running on, hdfs-namenode can perform its Data-Locality optimization Code on K8s (see doc)
*
* The output of "resolve" is a List of "K8s-Network-Adresses" in the Format:
*
*       default-rack/<kubernetes-node>/<Pod-IP>
*
* ,whereas default-rack is currently a constant
*
* */

public class PodToNodeMapping extends AbstractDNSToSwitchMapping {

    private KubernetesClient kubeclient;
    private Map<String, String> topologyMap;
    private String RACK_NAME = NetworkTopology.DEFAULT_RACK;
    private String topology_delimiter = "/";

    public static final String DEFAULT_NETWORK_LOCATION = NetworkTopology.DEFAULT_RACK + NetworkTopologyWithNodeGroup.DEFAULT_NODEGROUP;

    private static Log log = LogFactory.getLog(PodToNodeMapping.class);

    public PodToNodeMapping() {
        getOrCreateKubeClient();
    }

    public PodToNodeMapping(Configuration conf) {
        super(conf);
        getOrCreateKubeClient();
    }

    private KubernetesClient getOrCreateKubeClient() {
        if (kubeclient != null) {
            return kubeclient;
        }
        kubeclient = new DefaultKubernetesClient();
        return kubeclient;
    }


    @Override
    public List<String> resolve(List<String> names) {
        List<String> networkPathDirList = Lists.newArrayList();
        for (String name : names) {
            String networkPathDir = createNetAddress(name);
            networkPathDirList.add(networkPathDir);
        }
        if (log.isDebugEnabled()) {
            log.debug("[PTNM] Resolved " + names + " to " + networkPathDirList);
        }
        return ImmutableList.copyOf(networkPathDirList);
    }

    private String createNetAddress(String name) {
        String netAddress = "";
        String nodename = "";
        //Check if the Name is an IP-Adress or the Name of a physical Node (see above)
        if (topologyMap.get(name) == null && topologyMap.get(name).isEmpty()) {
            if (InetAddresses.isInetAddress(name)) {
                nodename = resolveByPodIP(name);
                topologyMap.put(name, nodename);
            } else {
                nodename = name.split("\\.")[0];
                topologyMap.put(name, nodename);
            }
            netAddress = RACK_NAME + topology_delimiter + nodename;
            return netAddress;
        } else {
            nodename = topologyMap.get(name);
            return nodename;
        }
    }

    private String resolveByPodIP(String podIP) {
        String Nodename = "";
//        get pods with given ip Adress from kubeapi
        List<Pod> podsWithIPAddress = kubeclient.pods().inAnyNamespace().withField("status.podIP", podIP).list().getItems();
        if (podsWithIPAddress.size() > 0) {
            Nodename = podsWithIPAddress.get(0).getSpec().getNodeName();
        }
        return Nodename;
    }

    @Override
    public void reloadCachedMappings() {

    }

    @Override
    public void reloadCachedMappings(List<String> names) {

    }


}