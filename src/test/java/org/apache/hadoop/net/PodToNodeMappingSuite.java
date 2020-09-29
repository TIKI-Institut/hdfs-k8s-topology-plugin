package org.apache.hadoop.net;

import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PodToNodeMappingSuite {

    final private KubernetesServer server = new KubernetesServer();

    @Before
    public void before() {
        server.before();
    }

    @After
    public void after() {
        server.after();
    }

    @Test
    public void runUnitTest() {
        String[] podIPs = {"10.233.115.144", "10.223.178.167"};
        String[] fqdns = {"kubernetes-worker-5.dsp.tiki.local", "kubernetes-worker-2.dsp.tiki.local"};

        List<String> rawLocalityInfo = new ArrayList<>();
        rawLocalityInfo.addAll(Arrays.asList(podIPs));
        rawLocalityInfo.addAll(Arrays.asList(fqdns));

        PodList firstPod = new PodListBuilder().withItems(new PodBuilder().withNewMetadata().withName("pod1")
                .endMetadata().withNewStatus().withPodIP(podIPs[0]).endStatus().withNewSpec()
                .withNodeName("kubernetes-worker-1").endSpec().build()).build();
        PodList secondPod = new PodListBuilder().withItems(new PodBuilder().withNewMetadata().withName("pod2")
                .endMetadata().withNewStatus().withPodIP(podIPs[1]).endStatus().withNewSpec()
                .withNodeName("kubernetes-worker-3").endSpec().build()).build();

        server.expect().get().withPath("/api/v1/pods?fieldSelector=status.podIP%3D" + podIPs[0])
                .andReturn(HttpURLConnection.HTTP_OK, firstPod).always();
        server.expect().get().withPath("/api/v1/pods?fieldSelector=status.podIP%3D" + podIPs[1])
                .andReturn(HttpURLConnection.HTTP_OK, secondPod).always();
        KubernetesClient client = server.getClient();

        PodToNodeMapping testInstance = new PodToNodeMapping() {
            @Override
            protected int getUpdateTime() {
                return 5;
            }

            @Override
            protected KubernetesClient getOrCreateKubeClient() {
                return client;
            }
        };
        assertEquals(new ArrayList<String>(Arrays.asList(netString("kubernetes-worker-1"),
                netString("kubernetes-worker-3"), netString("kubernetes-worker-5"),
                netString("kubernetes-worker-2"))), testInstance.resolve(rawLocalityInfo));
    }

    public static String netString(String s) {
        return PodToNodeMapping.RACK_NAME + "/" + s;
    }
}