package com.tiki.ai.hadoop.net;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.AbstractDNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A namenode topology plugin, that maps pods to Kubernetes Nodes to fix data locality
 * <p>
 * It resolves a network path of the following structure:
 * <p>
 * RACK    /   NODE-NAME   /   POD-IP or FQDN
 * <p>
 * where NODE-NAME is the name of the K8s-Node, the Pod is running on
 * In this implementation, the rack is always set to a default value
 * <p>
 * The resolve method uses an Kubernetes Client to retrieve nodenames from pod-IPs/FQDNs
 * Generally, the main methods recieves an FQDN in case of a Datanode-Registry and an IP in case of a Client-Request
 * Resolved nodenames are cached for a fixed amount of time
 *
 * @author Linus Meierhoefer
 */
public class PodToNodeMapping extends AbstractDNSToSwitchMapping {

    private static final Log log = LogFactory.getLog(PodToNodeMapping.class);
    private KubernetesClient kubeclient;
    private final String topology_delimiter = "/";
    protected static String RACK_NAME = NetworkTopology.DEFAULT_RACK;
    protected static String DEFAULT_NODE_NAME = "default-host";
    protected int updateTime = 300;
    private final String ENV_TOPOLOGY_UPDATE_IN_MIN = "TOPOLOGY_UPDATE_IN_MIN";
    protected Cache<String, String> cache;

    protected void init() {
        // Set Kubernetes Client
        getOrCreateKubeClient();
        // Start Cache
        if (System.getenv(ENV_TOPOLOGY_UPDATE_IN_MIN) != null) {
            try {
                setUpdateTime(Integer.parseInt(System.getenv(ENV_TOPOLOGY_UPDATE_IN_MIN)) * 60);
            } catch (NumberFormatException e) {
                log.warn("Error while parsing integer in env variable " + ENV_TOPOLOGY_UPDATE_IN_MIN, e);
            }
        }
        cache = CacheBuilder.newBuilder().expireAfterWrite(getUpdateTime(), TimeUnit.SECONDS).build();
    }

    public PodToNodeMapping() {
        init();
    }

    public PodToNodeMapping(Configuration conf) {
        super(conf);
        init();
    }

    protected KubernetesClient getOrCreateKubeClient() {
        if (kubeclient != null) {
            return kubeclient;
        }
        kubeclient = new DefaultKubernetesClient();
        return kubeclient;
    }

    protected int getUpdateTime() {
        return updateTime;
    }

    private void setUpdateTime(int updateTime) {
        this.updateTime = updateTime;
    }

    /**
     * This is the main entrypoint
     * The methods receives a List of raw Locality Information(either IP or FQDN)
     * @param names
     * @return
     */
    @Override
    public List<String> resolve(List<String> names) {
        List<String> networkPathDirList = Lists.newArrayList();
        kubeclient = getOrCreateKubeClient();
        for (String name : names) {
            try {
                String networkPathDir = createNetworkLocationPath(name);
                networkPathDirList.add(networkPathDir);
            } catch (ExecutionException e) {
                log.error("Error will parsing '" + name + "'. Skipping...", e);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Resolved " + names + " to " + networkPathDirList);
        }
        return ImmutableList.copyOf(networkPathDirList);
    }

    private String createNetworkLocationPath(String name) throws ExecutionException {
        String nodename = DEFAULT_NODE_NAME;
        //Check if Name is an IP-Address or FQDN
        if (InetAddresses.isInetAddress(name)) {
            if (cache.asMap().containsKey(name)) {
                log.debug("IP: " + name + " is cached with nodename: " + nodename);
                nodename = cache.get(name);
            } else {
                log.debug("IP: " + name + " is not cached");
                Optional<String> resolvedPodIP = getNodenameByIP(name);
                if (resolvedPodIP.isPresent()) {
                    nodename = resolvedPodIP.get();
                } else {
                    log.debug("Using default node name for IP: " + name);
                }

                cache.put(name, nodename);
            }
        } else {
            nodename = name.split("\\.")[0];
        }

        return RACK_NAME + topology_delimiter + nodename;
    }

    protected Optional<String> getNodenameByIP(String podIP) {
        try {
            String nodename = null;

            List<Pod> podsWithIPAddress = kubeclient.pods().inAnyNamespace().withField("status.podIP", podIP)
                    .list().getItems();
            if (podsWithIPAddress.size() > 0) {
                nodename = podsWithIPAddress.get(0).getSpec().getNodeName();
            }

            return Optional.ofNullable(nodename);
        } catch (KubernetesClientException e) {
            log.warn("Couldn't resolve Node by Pod IP " + podIP, e);
            return Optional.empty();
        }
    }

    @Override
    public void reloadCachedMappings() {
    }

    @Override
    public void reloadCachedMappings(List<String> names) {
    }
}