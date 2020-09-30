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
    public void resolveFQDN() {
        String[] fqdns = {"kubernetes-worker-6.dsp.example.local", "kubernetes-worker-2.dsp.example.local",
                "kubernetes-worker-4.dsp.example.local", "kubernetes-worker-9.dsp.example.local"};
        List<String> rawLocalityInfo = Arrays.asList(fqdns);

        PodToNodeMapping testInstance = new PodToNodeMapping();

        assertEquals(
                Arrays.asList(
                        netString("kubernetes-worker-6"),
                        netString("kubernetes-worker-2"),
                        netString("kubernetes-worker-4"),
                        netString("kubernetes-worker-9")),
                testInstance.resolve(rawLocalityInfo));
    }

    @Test
    public void resolveIP() {
        String[] podIPs = {"10.213.90.8", "10.67.78.2", "90.72.8.3", "43.4.21.1"};

        List<String> rawLocalityInfo = Arrays.asList(podIPs);

        PodList firstPod = new PodListBuilder().withItems(new PodBuilder().withNewMetadata().withName("pod1")
                .endMetadata().withNewStatus().withPodIP(podIPs[0]).endStatus().withNewSpec()
                .withNodeName("kubernetes-worker-1").endSpec().build()).build();
        PodList secondPod = new PodListBuilder().withItems(new PodBuilder().withNewMetadata().withName("pod2")
                .endMetadata().withNewStatus().withPodIP(podIPs[1]).endStatus().withNewSpec()
                .withNodeName("kubernetes-worker-3").endSpec().build()).build();
        PodList thirdPod = new PodListBuilder().withItems(new PodBuilder().withNewMetadata().withName("pod3")
                .endMetadata().withNewStatus().withPodIP(podIPs[2]).endStatus().withNewSpec()
                .withNodeName("kubernetes-worker-9").endSpec().build()).build();
        PodList fourthPod = new PodListBuilder().withItems(new PodBuilder().withNewMetadata().withName("pod4")
                .endMetadata().withNewStatus().withPodIP(podIPs[3]).endStatus().withNewSpec()
                .withNodeName("kubernetes-worker-6").endSpec().build()).build();

        server.expect().get().withPath("/api/v1/pods?fieldSelector=status.podIP%3D" + podIPs[0])
                .andReturn(HttpURLConnection.HTTP_OK, firstPod).always();
        server.expect().get().withPath("/api/v1/pods?fieldSelector=status.podIP%3D" + podIPs[1])
                .andReturn(HttpURLConnection.HTTP_OK, secondPod).always();
        server.expect().get().withPath("/api/v1/pods?fieldSelector=status.podIP%3D" + podIPs[2])
                .andReturn(HttpURLConnection.HTTP_OK, thirdPod).always();
        server.expect().get().withPath("/api/v1/pods?fieldSelector=status.podIP%3D" + podIPs[3])
                .andReturn(HttpURLConnection.HTTP_OK, fourthPod).always();

        PodToNodeMapping testInstance = new PodToNodeMapping() {
            @Override
            protected KubernetesClient getOrCreateKubeClient() {
                return server.getClient();
            }
        };

        assertEquals(
                Arrays.asList(
                        netString("kubernetes-worker-1"),
                        netString("kubernetes-worker-3"),
                        netString("kubernetes-worker-9"),
                        netString("kubernetes-worker-6")),
                testInstance.resolve(rawLocalityInfo));
    }

    @Test
    public void resolve() {
        String[] podIPs = {"10.233.115.144", "10.223.178.167"};
        String[] fqdns = {"kubernetes-worker-5.dsp.example.local", "kubernetes-worker-2.dsp.example.local"};

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

        PodToNodeMapping testInstance = new PodToNodeMapping() {
            @Override
            protected KubernetesClient getOrCreateKubeClient() {
                return server.getClient();
            }
        };

        assertEquals(
                Arrays.asList(
                        netString("kubernetes-worker-1"),
                        netString("kubernetes-worker-3"),
                        netString("kubernetes-worker-5"),
                        netString("kubernetes-worker-2")),
                testInstance.resolve(rawLocalityInfo));
    }

    public static String netString(String s) {
        return PodToNodeMapping.RACK_NAME + "/" + s;
    }
}