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
