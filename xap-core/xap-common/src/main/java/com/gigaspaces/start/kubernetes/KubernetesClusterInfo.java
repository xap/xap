package com.gigaspaces.start.kubernetes;

public class KubernetesClusterInfo {
    private static volatile KubernetesClusterInfo instance;
    private final String kubernetesServiceHost;
    private final String kubernetesServicePort;
    private final String kubernetesClusterId;
    private final boolean kubernetesServiceConfigured;

    public static KubernetesClusterInfo getInstance(){
        KubernetesClusterInfo snapshot = instance;
        if (snapshot != null)
            return snapshot;
        synchronized (KubernetesClusterInfo.class) {
            if (instance == null)
                instance = new KubernetesClusterInfo();
            return instance;
        }
    }

    private KubernetesClusterInfo() {
        this.kubernetesServiceHost = System.getenv("INTERNAL_K8S_SERVICE_HOST");
        this.kubernetesServicePort = System.getenv("INTERNAL_K8S_SERVICE_LRMI_PORT");
        this.kubernetesClusterId = System.getenv("INTERNAL_K8S_CLUSTER_ID");
        this.kubernetesServiceConfigured = validateString(kubernetesServiceHost) && validateString(kubernetesClusterId);
    }


    public String getKubernetesServiceHost() {
        return kubernetesServiceHost;
    }

    public String getKubernetesServicePort() {
        return kubernetesServicePort;
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
