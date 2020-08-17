package org.apache.hadoop.net;

import java.util.List;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import org.apache.hadoop.net.AbstractDNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetworkTopologyWithNodeGroup;

/*HDFS-Topology-Plugin which resolves K8s NodeNames (such as kubernetes-worker-2.dsp.tiki.local) and Pod-VIPS to their Kubernetes-Nodes*/

public class PodtoNodeMapping extends AbstractDNSToSwitchMapping{

    private KubernetesClient kubclient;
    private String RACK_NAME = NetworkTopology.DEFAULT_RACK;
    private String topology_delimiter = "/";
    private List<Pod> podsInNamespace;
    private String namespace = "hdfs-testcluster";

    public static final String DEFAULT_NETWORK_LOCATION = NetworkTopology.DEFAULT_RACK + NetworkTopologyWithNodeGroup.DEFAULT_NODEGROUP;

    private static Log log = LogFactory.getLog(PodtoNodeMapping.class);

    public PodtoNodeMapping() {
        getOrcreateKubClient();
        getPodsinNamespace();
    }

    public PodtoNodeMapping(Configuration conf){
        super(conf);
        getOrcreateKubClient();
        getPodsinNamespace();
    }

    private KubernetesClient getOrcreateKubClient() {
        if(kubclient != null) {return kubclient;}
        kubclient = new DefaultKubernetesClient();
        return kubclient;
    }

    private void  getPodsinNamespace(){
        podsInNamespace = kubclient.pods().inNamespace(namespace).list().getItems();
    }

    @Override
    public List<String> resolve(List<String> names) {
        getPodsinNamespace();
        List<String> networkPathDirList = Lists.newArrayList();
        for (String name: names) {
            getPodsinNamespace();
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
        getPodsinNamespace();
        //Check if the Name is an IP-Adress or the Name of a physical Node (see above)
        if (InetAddresses.isInetAddress(name)) {
            nodename = resolvebyPodIP(name);
        } else {
            nodename = name.split("\\.")[0];
        }
        netAddress = RACK_NAME + topology_delimiter + nodename;
        return netAddress;
    }

    private String resolvebyPodIP(String podIP) {
        String Nodename = "";
        getPodsinNamespace();
        for (Pod pod: podsInNamespace) {
                if (pod.getStatus().getPodIP().equals(podIP)) {
                    Nodename = pod.getSpec().getNodeName();
                    break;
                }
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