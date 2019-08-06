package com.gigaspaces.start.kubernetes;

import com.gigaspaces.start.XapNetworkInfo;

public class KuberntesClusterInfo {
    private static volatile KuberntesClusterInfo instance;
    private final String kubernetesServiceHost;
    private final String kubernetesClusterId;
    private final boolean kubernetesServiceConfigured;

    public static KuberntesClusterInfo getInstance(){
        KuberntesClusterInfo snapshot = instance;
        if (snapshot != null)
            return snapshot;
        synchronized (KuberntesClusterInfo.class) {
            if (instance == null)
                instance = new KuberntesClusterInfo();
            return instance;
        }
    }

    private KuberntesClusterInfo() {
        this.kubernetesServiceHost = System.getenv("XAP_KUBERNETES_HOST");
        this.kubernetesClusterId = System.getenv("KUBERNETES_CLUSTER_ID");
        this.kubernetesServiceConfigured = validateString(kubernetesServiceHost) && validateString(kubernetesClusterId);
    }


    public String getKubernetesServiceHost() {
        return kubernetesServiceHost;
    }

    public String getKubernetesClusterId() {
        return kubernetesClusterId;
    }

    public boolean isKubernetesServiceConfigured() {
        return kubernetesServiceConfigured;
    }

    private boolean validateString(String s){
        return s != null && !s.isEmpty() && !s.equals("null");
    }
}
