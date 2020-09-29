package org.apache.hadoop.net;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.time.temporal.ChronoUnit.MINUTES;

/*HDFS-Topology Plugin, which maps Pods to K8s-CusterNodes, on which they are running on
 *
 * The Method "resolve" gets called by CachedDNSToSwitchMapping and receives as Input either a DNS-String, in case of datanodes (eg. k8s-worker-1.org.local),
 * or a Pod-IP-Address (eg. 10.100.2.3)
 *
 * With additional Information about on which K8s-Node the datanode and client-pods are running on, hdfs-namenode can perform its Data-Locality optimization Code on K8s (see doc)
 *
 * The output of "resolve" is a List of "K8s-Network-Addresses" in the Format:
 *
 *       default-rack/<kubernetes-node>/<Pod-IP>
 *
 * ,whereas default-rack is currently a constant
 *
 * */

public class PodToNodeMapping extends AbstractDNSToSwitchMapping {

    private KubernetesClient kubeclient;
    protected ConcurrentHashMap<String, Pair<String, LocalTime>> topologyMap = new ConcurrentHashMap<String, Pair<String, LocalTime>>();
    protected static String RACK_NAME = NetworkTopology.DEFAULT_RACK;
    private String topology_delimiter = "/";
    private LocalTime originTime;
    protected int updateTime;

    public static final String DEFAULT_NETWORK_LOCATION = NetworkTopology.DEFAULT_RACK + NetworkTopologyWithNodeGroup.DEFAULT_NODEGROUP;

    private static Log log = LogFactory.getLog(PodToNodeMapping.class);

    private Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
            log.debug("[PTNM] Starting Thread MapWatcher");
            updateTime = getUpdateTime();
            while (true) {
                if ((MINUTES.between(originTime, LocalTime.now())) > updateTime) {
                    for (Map.Entry<String, Pair<String, LocalTime>> entry : topologyMap.entrySet()) {
                        if ((MINUTES.between(entry.getValue().getRight(), LocalTime.now())) > updateTime) {
                            log.debug("[PTNM]" + entry.getKey() + " is going to be removed");
                            topologyMap.remove(entry.getKey());
                        }
                    }
                    // Override originTime
                    originTime = LocalTime.now();
                }
            }
        }
    });

    public PodToNodeMapping() {
        originTime = LocalTime.now();
        log.debug("[PTNM] Started PodToNodeMapping at " + originTime);
        getOrCreateKubeClient();
        t.setDaemon(true);
        t.start();
    }

    public PodToNodeMapping(Configuration conf) {
        super(conf);
        originTime = LocalTime.now();
        log.debug("[PTNM] Started PodToNodeMapping at " + originTime);
        getOrCreateKubeClient();
        t.setDaemon(true);
        t.start();
    }

    protected KubernetesClient getOrCreateKubeClient() {
        if (kubeclient != null) {
            return kubeclient;
        }
        kubeclient = new DefaultKubernetesClient();
        return kubeclient;
    }

    protected int getUpdateTime() {
        if (System.getenv("TOPOLOGY_UPDATE_IN_MIN") != null) {
            return Integer.parseInt(System.getenv("TOPOLOGY_UPDATE_IN_MIN"));
        } else {
            return 5;
        }
    }

    @Override
    public List<String> resolve(List<String> names) {
        List<String> networkPathDirList = Lists.newArrayList();
        kubeclient = getOrCreateKubeClient();
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
        //Check if the Name is an IP-Address or the Name of a physical Node (see above)
        log.debug("[PTNM] Checking if identifier " + name + " is cached");
        if (!topologyMap.containsKey(name)) {
            log.debug("[PTNM] Identifier " + name + " is not cached");
            if (InetAddresses.isInetAddress(name)) {
                nodename = resolveByPodIP(name);
                topologyMap.put(name, new ImmutablePair(nodename, LocalTime.now()));
            } else {
                nodename = name.split("\\.")[0];
                topologyMap.put(name, new ImmutablePair(nodename, LocalTime.now()));
            }
            netAddress = RACK_NAME + topology_delimiter + nodename;
            return netAddress;
        } else {
            nodename = topologyMap.get(name).getLeft();
            log.debug("[PTNM] Identifier " + name + " is cached with nodename " + nodename);
            netAddress = RACK_NAME + topology_delimiter + nodename;
            return netAddress;
        }
    }

    private String resolveByPodIP(String podIP) {
        String Nodename = "";
        // get pods with given ip Address from kubeapi
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