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
package org.jini.rio.boot;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Barak Bar Orion on 11/26/15.
 *
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class TLSUtils {
    public static boolean wasInit;

    static HostnameVerifier hostnameVerifier = new HostnameVerifier() {
        public boolean verify(String urlHostName, SSLSession session) {
            return true;
        }
    };

    public static synchronized void enableHttpsClient() {
        if (!wasInit) {
            wasInit = true;
            if (Boolean.getBoolean("org.jini.rio.tools.webster.tls")) {
                Logger logger = LoggerFactory.getLogger(TLSUtils.class.getName());
                try {
                    SSLContext sc = SSLContext.getInstance("TLS");
                    sc.init(null,
                            new TrustManager[]{
                                    new X509TrustManager() {
                                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                                            return null;
                                        }

                                        public void checkClientTrusted(
                                                java.security.cert.X509Certificate[] certs, String authType) {
                                        }

                                        public void checkServerTrusted(
                                                java.security.cert.X509Certificate[] certs, String authType) {
                                        }
                                    }
                            }
                            , new java.security.SecureRandom());
                    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
                    HttpsURLConnection.setDefaultHostnameVerifier(hostnameVerifier);
                    logger.info("Replaced HttpsURLConnection.setDefaultSSLSocketFactory and .setDefaultHostnameVerifier");
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

}
