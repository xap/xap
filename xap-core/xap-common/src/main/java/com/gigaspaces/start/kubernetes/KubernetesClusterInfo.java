/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
