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

/*HDFS-Topology-Plugin which resolves K8s NodeNames (such as kubernetes-worker-2.dsp.tiki.local) and Pod-VIPS to their Kubernetes-Nodes*/

public class PodToNodeMapping extends AbstractDNSToSwitchMapping {

    private KubernetesClient kubeclient;
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
        if (InetAddresses.isInetAddress(name)) {
            nodename = resolveByPodIP(name);
        } else {
            nodename = name.split("\\.")[0];
        }
        netAddress = RACK_NAME + topology_delimiter + nodename;
        return netAddress;
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