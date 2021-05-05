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

package com.gigaspaces.start;

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.start.kubernetes.KubernetesClusterInfo;
import com.gigaspaces.start.manager.XapManagerClusterInfo;
import com.gigaspaces.start.manager.XapManagerConfig;
import com.gigaspaces.time.AbsoluteTime;
import com.gigaspaces.time.ITimeProvider;

import net.jini.core.discovery.LookupLocator;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import static com.gigaspaces.CommonSystemProperties.SYSTEM_TIME_PROVIDER;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class SystemInfo {

    public static final String LOOKUP_GROUPS_SYS_PROP = "com.gs.jini_lus.groups";
    public static final String LOOKUP_LOCATORS_SYS_PROP = "com.gs.jini_lus.locators";

    private static final SystemInfo instance = new SystemInfo();

    private final XapLookup lookup;
    private final XapNetworkInfo network;
    private final XapOperatingSystem os;
    private final XapTimeProvider timeProvider;
    private final XapManagerClusterInfo managerClusterInfo;
    private final KubernetesClusterInfo kubernetesClusterInfo;

    public static SystemInfo singleton() {
        return instance;
    }

    private SystemInfo() {
        this.os = new XapOperatingSystem();
        this.network = XapNetworkInfo.getInstance();
        this.timeProvider = new XapTimeProvider();
        this.managerClusterInfo = new XapManagerClusterInfo(network.getHost());
        this.kubernetesClusterInfo = KubernetesClusterInfo.getInstance();
        this.lookup = new XapLookup(managerClusterInfo);
    }

    public XapLookup lookup() {
        return lookup;
    }

    public XapNetworkInfo network() {
        return network;
    }

    public KubernetesClusterInfo kubernetes(){
        return kubernetesClusterInfo;
    }

    public XapOperatingSystem os() {
        return os;
    }

    public XapManagerClusterInfo getManagerClusterInfo() {
        return managerClusterInfo;
    }

    public XapTimeProvider timeProvider() {
        return timeProvider;
    }

    public static class XapLookup {

        private static final String SEPARATOR = ",";
        private String groups;
        private String[] groupsArray;
        private String locators;
        private LookupLocator[] locatorsArray;

        private XapLookup(XapManagerClusterInfo managerClusterInfo) {
            setGroups(System.getProperty(LOOKUP_GROUPS_SYS_PROP, GsEnv.get("LOOKUP_GROUPS")));
            String explicitLocators = System.getProperty(LOOKUP_LOCATORS_SYS_PROP, GsEnv.get("LOOKUP_LOCATORS"));
            String managerLocators = toLocators(managerClusterInfo);
            if (!BootIOUtils.isEmpty(managerLocators) && !BootIOUtils.isEmpty(explicitLocators) && !managerLocators.equals(explicitLocators))
                throw new IllegalStateException("Ambiguous locators: Manager locators: [" + managerLocators +"], explicit locators: [" + explicitLocators + "]");
            setLocators(!managerLocators.isEmpty() ? managerLocators : explicitLocators);
        }

        private static String toLocators(XapManagerClusterInfo managerClusterInfo) {
            String result = "";
            for (XapManagerConfig managerConfig : managerClusterInfo.getServers()) {
                String locator = managerConfig.getHost();
                if (managerConfig.getLookupService() != null)
                    locator += ":" + managerConfig.getLookupService();
                result = result.isEmpty() ? locator : result + SEPARATOR + locator;
            }
            return result;
        }

        public String defaultGroups() {
            return "xap-" + PlatformVersion.getVersion();
        }

        public String groups() {
            return groups;
        }

        public String[] groupsArray() {
            return groupsArray;
        }

        public String locators() {
            return locators;
        }

        public LookupLocator[] locatorsArray() {
            return locatorsArray;
        }

        public String setGroups(String groups) {
            String prevValue = this.groups;
            if (groups != null)
                groups = groups.trim();
            if (groups == null || groups.length() == 0)
                groups = defaultGroups();
            this.groups = groups;
            setSystemProperty(LOOKUP_GROUPS_SYS_PROP, groups);

            List<String> groupsList = toList(groups, SEPARATOR);
            this.groupsArray = groupsList.toArray(new String[groupsList.size()]);
            return prevValue;
        }

        public void setGroups(String[] lookupGroups) {
            setGroups(join(lookupGroups, SEPARATOR));
        }

        public String setLocators(String locators) {
            String prevValue = this.locators;
            this.locators = locators;
            setSystemProperty(LOOKUP_LOCATORS_SYS_PROP, locators);

            List<String> locatorsList = toList(locators, SEPARATOR);
            this.locatorsArray = new LookupLocator[locatorsList == null ? 0 : locatorsList.size()];
            for (int i = 0; i < locatorsArray.length; i++) {
                try {
                    locatorsArray[i] = new LookupLocator("jini://" + locatorsList.get(i));
                } catch (MalformedURLException e) {
                    throw new IllegalStateException("Failed to generate locators for " + locatorsList.get(i), e);
                }
            }
            return prevValue;
        }

        private static String join(String[] array, String separator) {
            if (array == null)
                return null;
            if (array.length == 0)
                return "";
            if (array.length == 1)
                return array[0];
            StringBuilder sb = new StringBuilder(array[0]);
            for (int i = 1; i < array.length; i++)
                sb.append(separator).append(array[i]);
            return sb.toString();
        }

        private static List<String> toList(String s, String separator) {
            if (s == null)
                return null;
            ArrayList<String> result = new ArrayList<String>();
            StringTokenizer tokenizer = new StringTokenizer(s, separator);
            while (tokenizer.hasMoreTokens())
                result.add(tokenizer.nextToken().trim());
            return result;
        }

        private static String setSystemProperty(String key, String value) {
            return value != null ? System.setProperty(key, value) : System.clearProperty(key);
        }
    }

    public static class XapOperatingSystem {
        private final long processId = JavaUtils.getPid();
        private final String username = JavaUtils.getUsername();
        private final boolean isWindows = JavaUtils.isWindows();

        public long processId() {
            return processId;
        }

        public String getUsername() {
            return username;
        }

        public boolean isWindows() {
            return isWindows;
        }
    }

    public static class XapTimeProvider {
        private final ITimeProvider _timeProvider;

        public XapTimeProvider() {
            final String timeProviderClassName = System.getProperty(SYSTEM_TIME_PROVIDER);
            ;
            if (timeProviderClassName == null || timeProviderClassName.length() == 0) {
                _timeProvider = new AbsoluteTime();

            } else {
                try {
                    final Class timeProviderClass = Class.forName(timeProviderClassName);
                    final Object timeProvider = timeProviderClass.newInstance();
                    _timeProvider = (ITimeProvider) timeProvider;
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Unable to load time-provider: " + timeProviderClassName +
                            " - verify that this provider class is in the classpath", e);
                } catch (Throwable e) {
                    throw new RuntimeException("Unable to load time-provider: " + timeProviderClassName, e);
                }
            }
        }

        /**
         * @return TimeProvider class name of object representing this provider
         */
        public String getTimeProviderName() {
            return _timeProvider.getClass().getName();
        }

        /**
         * @return <tt>true</tt> if time-provider is of RelativeTime
         */
        public boolean isRelativeTime() {
            return _timeProvider.isRelative();
        }

        public long timeMillis() {
            return _timeProvider.timeMillis();
        }
    }
}
