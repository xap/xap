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

package com.gigaspaces.internal.jmx;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.management.entry.JMXConnection;
import com.gigaspaces.start.SystemBoot;
import com.gigaspaces.start.SystemInfo;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;


@com.gigaspaces.api.InternalApi
public class JMXUtilities {

    public static JMXConnection createJMXConnectionAttribute(String serviceName) {

        String url = SystemBoot.getJMXServiceURL();
        if(SystemInfo.singleton().network().isPublicHostConfigured()){
            url= System.getProperty(CommonSystemProperties.JMX_PUBLIC_SERVICE_URL);
        }
        return url == null ? null : new JMXConnection(url, serviceName + "_" +
                SystemInfo.singleton().network().getPublicHostId() + "_" + SystemBoot.getRegistryPort());
    }

    public static String createJMXUrl(String hostName, int port) {
        if (isHostNameOfIpv6AndNeedsSquareBrackets(hostName))
            hostName = "[" + hostName + "]";
        return createJMXUrl(hostName + ":" + port);
    }

    //This is kind of a work around to support ipv6, there are too many places in the code which calls this method to go over now and try to fix all of them
    private static boolean isHostNameOfIpv6AndNeedsSquareBrackets(String hostName) {
        try {
            InetAddress inetAddress = InetAddress.getByName(hostName);
            return inetAddress instanceof Inet6Address && !hostName.startsWith("[");

        } catch (UnknownHostException e) {
            return hostName.contains(":") && !hostName.startsWith("[");
        }
    }

    public static String createJMXUrl(String jndiURL) {
        jndiURL = convertJndiURLtoIpV6Compliant(jndiURL);
        return "service:jmx:rmi:///jndi/rmi://" + jndiURL + "/jmxrmi";
    }

    private static String convertJndiURLtoIpV6Compliant(String jndiURL) {
        if (jndiURL.indexOf(":") == jndiURL.lastIndexOf(":"))
            return jndiURL;

        String hostName = jndiURL.substring(0, jndiURL.lastIndexOf(":"));
        if (!isHostNameOfIpv6AndNeedsSquareBrackets(hostName))
            return jndiURL;

        String port = jndiURL.substring(jndiURL.lastIndexOf(":") + 1);
        return "[" + hostName + "]:" + port;
    }
}