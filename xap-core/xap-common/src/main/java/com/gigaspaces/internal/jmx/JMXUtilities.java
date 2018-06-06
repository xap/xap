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

//        return url == null ? null : new JMXConnection(url, serviceName + "_" +
//                SystemInfo.singleton().network().getHostId() + "_" + SystemBoot.getRegistryPort());
        System.out.println("in createJMXConnectionAttribute for service "+ serviceName +", url=" + url + " hostPublic="+SystemInfo.singleton().network().getPublicHostId());
        return url == null ? null : new JMXConnection(url, serviceName + "_" +
                SystemInfo.singleton().network().getPublicHostId() + "_" + SystemBoot.getRegistryPort());
    }

    public static String createJMXUrl(String hostName, int port) {
        System.out.println("33333");
        if(SystemInfo.singleton().network().isPublicIpConfigure()){
            System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX- public ip configured set it");
            hostName=SystemInfo.singleton().network().getPublicHost().getHostName();
        }

        if (isHostNameOfIpv6AndNeedsSquareBrackets(hostName))
            hostName = "[" + hostName + "]";
        return createJMXUrl(hostName + ":" + port);
    }

    //This is kind of a work around to support ipv6, there are too many places in the code which calls this method to go over now and try to fix all of them
    private static boolean isHostNameOfIpv6AndNeedsSquareBrackets(String hostName) {
        System.out.println("in isHostName, hostName=" + hostName);
        try {
            InetAddress inetAddress = InetAddress.getByName(hostName);
            return inetAddress instanceof Inet6Address && !hostName.startsWith("[");

        } catch (UnknownHostException e) {
            return hostName.contains(":") && !hostName.startsWith("[");
        }
    }

    public static String createJMXUrl(String jndiURL) {
        System.out.println("111");
        jndiURL = convertJndiURLtoIpV6Compliant(jndiURL);
        System.out.println("XXXXXXXXXXXXXXXXXXXX jndiUrl="+jndiURL);
        return "service:jmx:rmi:///jndi/rmi://" + jndiURL + "/jmxrmi";
    }

    private static String convertJndiURLtoIpV6Compliant(String jndiURL) {
        System.out.println("2222");
        if (jndiURL.indexOf(":") == jndiURL.lastIndexOf(":"))
            return jndiURL;
        System.out.println("2222 return " + jndiURL);

        String hostName = jndiURL.substring(0, jndiURL.lastIndexOf(":"));
        if (!isHostNameOfIpv6AndNeedsSquareBrackets(hostName)){
            System.out.println("2222 return " + jndiURL);
            return jndiURL;

        }

        String port = jndiURL.substring(jndiURL.lastIndexOf(":") + 1);
        System.out.println("2222 return [" + hostName + "]:" + port);
        return "[" + hostName + "]:" + port;
    }
}