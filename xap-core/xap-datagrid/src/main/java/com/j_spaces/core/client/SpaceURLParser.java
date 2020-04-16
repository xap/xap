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

package com.j_spaces.core.client;

import com.gigaspaces.config.ConfigurationException;
import com.gigaspaces.internal.lookup.SpaceUrlUtils;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.Constants;
import com.j_spaces.core.ISpaceState;
import com.j_spaces.core.JSpaceState;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.XPathProperties;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
@com.gigaspaces.api.InternalApi
public class SpaceURLParser {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACE_URL);
    private static final String DEFAULT_RMID_PORT = "10098";

    /* prevent to instance this class */
    private SpaceURLParser() {
    }

    /**
     * Parse given URL.
     *
     * @param spaceURL space url string
     * @return spaceURL object which represents the passed space url string.
     **/
    public static SpaceURL parseURL(String spaceURL)
            throws MalformedURLException {
        return parseURL(spaceURL, null);
    }

    /**
     * Parse given space url string and return a full SpaceURL object.
     *
     * @return spaceURL object which represents the passed space url string.
     **/
    public static SpaceURL parseURL(String spaceURLStr, Properties customProperties)
            throws MalformedURLException {
        spaceURLStr = initialValidate(spaceURLStr);

        /*************  INITIAL URL CRUNCHING	**************************************************
         * SUPPORTING GIGASPACES backwards/old versions of 4.x and new 5.x space url semantics  */

        int minToken = 3;
        String containerName = null;
        String spaceName = null;
        String unicastHost = null;
        String protocol = null;
        String schemaName = null;
        String machineName = "localhost";

        int containerDotNotationIndex = spaceURLStr.indexOf("/./");
        int spaceNameDotNotationIndex = containerDotNotationIndex + 3;
        if (containerDotNotationIndex != -1)
            minToken++;

        /**
         * if the space url is of SpaceFinder.find("/./mySpace")
         * The space url above uses "." as the container name in the cache url.
         * A value of "." as the container name will be translated to the following pattern <space name>_container.
         * In the above example the container name will be mySpace_container.
         *
         * When a url is provided without the protocol and host name ("java://localhost:10098")
         * then SpaceFinder will expand the /./mySpace
         * to java://localhost:10098/mySpace_container/mySpace?schema=default.
         */
        if (containerDotNotationIndex == 0) {
            protocol = SpaceURL.EMBEDDED_SPACE_PROTOCOL;//always java:// protocol
            final String hostName = SystemInfo.singleton().network().getHostId();
            try {
                //get IP host address
                machineName = InetAddress.getByName(hostName).getHostName();
            } catch (UnknownHostException e) {
                if (_logger.isTraceEnabled())
                    _logger.trace(e.toString(), e);
            }

            unicastHost = hostName + ":" + DEFAULT_RMID_PORT; //always localhost:10098 host/port
            int endOfSpaceIndx = spaceURLStr.indexOf('?');
            if (endOfSpaceIndx != -1) {
                //we have a /./mySpace?SOME_MORE_ATTRIBS string
                spaceName = spaceURLStr.substring(spaceNameDotNotationIndex,
                        endOfSpaceIndx);// mySpace is the space/cache name
                int indexOfPotential_container = spaceURLStr.indexOf("_container");
                if (indexOfPotential_container != -1) {
                    //the space name contains also the container name (IF SCHEMA WAS NOT PASSED)
                    spaceName = spaceURLStr.substring(indexOfPotential_container + 11 /*end of _container index*/,
                            endOfSpaceIndx);// mySpace is the space/cache name
                }
            } else {
                //we have a /./mySpace string
                spaceName = spaceURLStr.substring(spaceNameDotNotationIndex);// mySpace is the space/cache name
                int indexOfPotential_container = spaceURLStr.indexOf("_container");
                if (indexOfPotential_container != -1) {
                    //the space name contains also the container name (IF SCHEMA WAS NOT PASSED)
                    spaceName = spaceURLStr.substring(indexOfPotential_container + 11 /*end of _container indx*/,
                            spaceURLStr.length());// mySpace is the space/cache name
                }
            }
            containerName = spaceName + "_container"; // mySpace_container is the container name
            schemaName = SpaceURL.DEFAULT_SCHEMA_NAME;
        } else {
            StringTokenizer st = new StringTokenizer(spaceURLStr, "/");
            int countTokens = st.countTokens();

            // check if valid url
            if (countTokens < minToken)
                throw new SpaceURLValidationException("Invalid space url - " + spaceURLStr);

            // get protocol
            protocol = st.nextToken();

            // throws <code>MalformedURLException</code> if unknown protocol
            if (!SpaceURL.isValidProtocol(protocol))
                throw new SpaceURLValidationException("Invalid space url - Unknown protocol found in space url: " + spaceURLStr);

            // get unicast host
            //GERSHON 08/08/06 FIX APP-322 bug
            //support when passing only host without port, add the default
            //port, so java://localhost will be set as java://localhost:10098.
            //that case is handled only if * is not used as the host
            String host = st.nextToken();
            if (host.equals(SpaceURL.ANY) || protocol.equalsIgnoreCase(SpaceURL.JINI_PROTOCOL))
                unicastHost = host;
            else //for RMI or embedded protocol
                unicastHost = getRMIDurl(host);

            if (containerDotNotationIndex != -1) {
                spaceName = st.nextToken("?");
                //GERSHON 08/08/06 FIX APP-322 bug
                //support the following space url java://localhost/./
                if (spaceName.startsWith("/./")) {
                    //if space name is prefixed with the
                    //container dot notation e.g. /./mySpace and without ?
                    // chop the prefix /./ and have only the space name
                    spaceName = spaceName.substring(3);
                } else if (spaceName.startsWith("/")) {
                    //if space name is prefixed without the
                    //container dot notation e.g. /mySpace and without ?
                    //chop the prefix / and have only the space name
                    spaceName = spaceName.substring(1);
                }

                containerName = spaceName + "_container";

                schemaName = SpaceURL.DEFAULT_SCHEMA_NAME;
                //             check if URL not equal to protocol://host/* otherwise MalformedURLException will be throw
                if (countTokens == minToken && containerName.equals(SpaceURL.ANY))
                    throw new SpaceURLValidationException("Invalid space url - " + spaceURLStr
                            + " container name can not be equals to *");

                // check that URL can't contain RMI protocol and container name = *
                if (containerName.equals(SpaceURL.ANY)
                        && protocol.equalsIgnoreCase(SpaceURL.RMI_PROTOCOL))
                    throw new SpaceURLValidationException("Invalid space url - " + spaceURLStr
                            + " URL can not be RMI protocol and container name equals to *");

                // if rmihost == * the default host will be container name
                if (protocol.equalsIgnoreCase(SpaceURL.RMI_PROTOCOL)
                        && unicastHost.equals(SpaceURL.ANY))
                    unicastHost = getRMIDurl(containerName);
                else if (protocol.equalsIgnoreCase(SpaceURL.RMI_PROTOCOL)
                        && !unicastHost.equals(SpaceURL.ANY))
                    unicastHost = getRMIDurl(unicastHost);
            } else {
                containerName = st.nextToken();

                // get container name
                if (countTokens == minToken && containerName.indexOf('?') != -1)
                    containerName = containerName.substring(0,
                            containerName.indexOf('?'));

                // check if URL not equal to protocol://host/* otherwise MalformedURLException will be throw
                if (countTokens == minToken && containerName.equals(SpaceURL.ANY))
                    throw new SpaceURLValidationException("Invalid space url - " + spaceURLStr
                            + " container name can not be equals to *");

                // check that URL can't contain RMI protocol and container name = *
                if (containerName.equals(SpaceURL.ANY)
                        && protocol.equalsIgnoreCase(SpaceURL.RMI_PROTOCOL))
                    throw new SpaceURLValidationException("Invalid space url - " + spaceURLStr
                            + " URL can not be RMI protocol and container name equals to *");

                // if rmihost == * the default host will be container name
                if (protocol.equalsIgnoreCase(SpaceURL.RMI_PROTOCOL)
                        && unicastHost.equals(SpaceURL.ANY))
                    unicastHost = getRMIDurl(containerName);
                else if (protocol.equalsIgnoreCase(SpaceURL.RMI_PROTOCOL)
                        && !unicastHost.equals(SpaceURL.ANY))
                    unicastHost = getRMIDurl(unicastHost);
            }

            // get space name, if we do NOT use the dot container notation
            if (st.hasMoreTokens() && containerDotNotationIndex == -1) {
                spaceName = st.nextToken("?");
                spaceName = (spaceName.startsWith("/") ? spaceName.substring(1)
                        : spaceName);
            }
        }

        //create the SpaceURL object and then fill it with required values after
        //parsing the space url string.
        SpaceURL spaceURL = new SpaceURL();
        spaceURL.initialize(spaceURLStr);

        if (protocol.equals(SpaceURL.EMBEDDED_SPACE_PROTOCOL))
            spaceURL.setProperty(SpaceURL.MACHINE_HOSTNAME, machineName);

        boolean ignoreValidation = Boolean.valueOf(spaceURL.getProperty(SpaceURL.IGNORE_VALIDATION));

        //check url basic validation
        try {
            if (!ignoreValidation)
                SpaceURLValidator.validate(spaceURL);
        } catch (SpaceURLValidationException e) {
            throw e;
        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(
                        "Space URL validation failed. Please check the error and fix the SpaceURL, since it might have serious implications on the initialized topology. ",
                        e);
            }
        }

        /** *********** HANDLE PROPERTIES */
        String propertiesFileName = spaceURL.getProperty(SpaceURL.PROPERTIES_FILE_NAME);
        // we first set the custom properties object which was passed, then
        // IF a space url properties attribute was set we load it (we load the
        // *.properties file)
        // and its values might OVERWRITE the currently existing properties.
        if (customProperties != null) {
            // if custom properties object was passed AND the
            // gs.space.url.arg.properties key is set
            // inside the object --> then we use it as the properties file name
            // in that case it will overwrite potential properties url attribute
            // passed as part of teh original space url string.
            String propFileName = customProperties.getProperty(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.PROPERTIES_FILE_NAME));
            if (!JSpaceUtilities.isEmpty(propFileName))
                propertiesFileName = propFileName;
            spaceURL.getCustomProperties().putAll(customProperties);
        }

        try {
            customProperties = findAndSetCustomProperties(propertiesFileName, spaceURL);
        } catch (ConfigurationException ce) {
            throw new MalformedURLException(ce.toString());
        } catch (Throwable e) {
            //might that we do not have security grant permission for setting/getting sys props
            //in that case we do nothing and just notify about it.
            if (_logger.isInfoEnabled()) {
                _logger.info("Could not set properties loaded from < "
                        + propertiesFileName + " > custom properties file: "
                        + e.getMessage());
            }
        }

        // Gershon: 02/08/06
        // If the url contains cluster_schema attribute but NOT the
        // schema attribute, we will implicitly add schema=default
        // that covers senarious that DONT use /./ notation
        if (spaceURL.containsKey(SpaceURL.CLUSTER_SCHEMA) && !(spaceURL.containsKey(SpaceURL.SCHEMA_NAME))) {
            schemaName = SpaceURL.DEFAULT_SCHEMA_NAME;
            spaceURL.setProperty(SpaceURL.SCHEMA_NAME, schemaName);
        }

        //GERSHON 30/08/06 - APP-489
        if (!ignoreValidation)
            SpaceURLValidator.validateClusterSchemaAttributes(spaceURL);

        /*************  special treatment for EMBEDDED_SPACE_PROTOCOL    *******************************************/
        if (protocol.equalsIgnoreCase(SpaceURL.EMBEDDED_SPACE_PROTOCOL)) {
            String clusterSchemaName = spaceURL.getProperty(SpaceURL.CLUSTER_SCHEMA);
            if (clusterSchemaName != null) {
                //if we have a cluster setup, then we use a <space-name>_container<id> notation for the container name
                if (spaceURL.containsKey(SpaceURL.CLUSTER_MEMBER_ID)) {
                    String memberID = spaceURL.getProperty(SpaceURL.CLUSTER_MEMBER_ID);
                    //append the <id> to the container name
                    //now the container will look like:
                    // <cache name>_container<id>
                    containerName += memberID;
                }
                //check if we need to use a backup id as well (backup_id)
                String backupMemberID = spaceURL.getProperty(SpaceURL.CLUSTER_BACKUP_ID);
                if (backupMemberID != null) {
                    //append the <backup_id> to the container name
                    //now the container will look like:
                    // <cache name>_container<id>_<backup_id>
                    containerName += '_' + backupMemberID;
                }
            }

            /*************  HANDLE RMIRegistry settings  **************************************************/
            /**
             * Parse valid JNDI port. If JNDI port really valid and later on, while container init
             * trying to startRMIRegistry on this port. Any exception during start is ignored.
             **/
            if (unicastHost != null && unicastHost.indexOf(':') != -1) {
                final String jndiEnabledXPath = customProperties.getProperty(XPathProperties.CONTAINER_JNDI_ENABLED);
                if (JSpaceUtilities.isEmpty(jndiEnabledXPath) || jndiEnabledXPath.equalsIgnoreCase("true"))
                    customProperties.setProperty(XPathProperties.CONTAINER_JNDI_ENABLED, "true");
                else if (jndiEnabledXPath.equalsIgnoreCase("false"))
                    customProperties.setProperty(XPathProperties.CONTAINER_JNDI_ENABLED, "false");

                final String jndiURL = customProperties.getProperty(XPathProperties.CONTAINER_JNDI_URL);
                if (JSpaceUtilities.isEmpty(jndiURL)) {
                    //Resolves GS-3127, added by Evgeny on 7.10.2007
                    //if appropriate system variable is not passed also
                    if (System.getProperty(SystemProperties.LOOKUP_JNDI_URL) != null)
                        unicastHost = System.getProperty(SystemProperties.LOOKUP_JNDI_URL);

                    //if no overrides exists, we set the host to the fallback which is
                    // localhost:10098
                    customProperties.setProperty(XPathProperties.CONTAINER_JNDI_URL, unicastHost);
                } else {
                    customProperties.setProperty(XPathProperties.CONTAINER_JNDI_URL, jndiURL);
                }
            }// end of rmi registry flag settings
        }//EMBEDDED_SPACE_PROTOCOL

        //if statement added by Evgeny on 16.10.2007 in order tp revent situation
        //when cluster members urls have JNDI as locator, this bug was
        //discovered by last changes to locatos tests
        if (!protocol.equalsIgnoreCase(SpaceURL.JINI_PROTOCOL)) {
            if (customProperties.getProperty(XPathProperties.CONTAINER_JNDI_URL) != null)
                unicastHost = customProperties.getProperty(XPathProperties.CONTAINER_JNDI_URL);
            else if (System.getProperty(SystemProperties.LOOKUP_JNDI_URL) != null)
                unicastHost = System.getProperty(SystemProperties.LOOKUP_JNDI_URL);
        }

        initialize(spaceURL, protocol, containerName, spaceName, schemaName, unicastHost);
        return spaceURL;
    }

    private static String initialValidate(String url)
            throws SpaceURLValidationException {
        //remove invalid spaces and Inverted Commas as part of SpaceURL
        url = JSpaceUtilities.removeInvertedCommas(url);
        url = JSpaceUtilities.removeDelimiter(url, ' ', '{', '}');
        if (_logger.isDebugEnabled())
            _logger.debug("Using SpaceURL: " + url);

        int first = url.indexOf('?');
        if (first != url.lastIndexOf('?'))
            throw new SpaceURLValidationException("Invalid space url - The space url has more than one '?' character");
        if (first == -1 && url.indexOf('&') > -1)
            throw new SpaceURLValidationException("Invalid space url - The space url contains the '&' delimiter but is missing the '?' character. Make sure the '?' character is set after the space/container name.");
        return url;
    }

    private static void initialize(SpaceURL spaceURL, String protocol, String containerName, String spaceName,
                                   String schemaName, String unicastHost) {
        spaceURL.setProperty(SpaceURL.PROTOCOL_NAME, protocol);
        spaceURL.setContainerName(containerName);
        spaceURL.setSpaceName(spaceName);
        spaceURL.setProperty(SpaceURL.MEMBER_NAME, containerName + ":" + spaceURL.getSpaceName());
        if (schemaName != null && !spaceURL.containsKey(SpaceURL.SCHEMA_NAME))
            spaceURL.setProperty(SpaceURL.SCHEMA_NAME, schemaName);
        spaceURL.setProperty(SpaceURL.HOST_NAME, unicastHost);

        if (!spaceURL.getProtocol().equalsIgnoreCase(SpaceURL.RMI_PROTOCOL)) {
            handleLocators(spaceURL);
            handleGroups(spaceURL);
        }

        if (spaceName != null) {
            if (!spaceURL.containsKey(SpaceURL.STATE))
                spaceURL.setProperty(SpaceURL.STATE, JSpaceState.convertToString(ISpaceState.STARTED));
        }
    }

    /**
     * Possible inputs for the locators can be any combination of the following: 1. SpaceURL
     * locators attribute e.g. {@link com.j_spaces.core.client.SpaceURL#LOCATORS} 2. properties
     * XPATH key e.g. {@link com.j_spaces.kernel.XPathProperties#CONTAINER_JINI_LUS_UNICAST_HOSTS}
     * 3. System property key e.g. {@link com.j_spaces.kernel.SystemProperties#JINI_LUS_LOCATORS} 4.
     * SpaceURL hosts might have also a list of host:port pairs in case it is a <code>jini://</code>
     * protocol ONLY. In general we take all the values from these inputs and merge them into one
     * locators list.
     */
    static void handleLocators(SpaceURL spaceURL) {
        Properties customProperties = spaceURL.getCustomProperties();

        String sysPropLocators = SystemInfo.singleton().lookup().locators();
        String xpathLocators = customProperties.getProperty(XPathProperties.CONTAINER_JINI_LUS_UNICAST_HOSTS);
        String spaceURLLocators = spaceURL.getProperty(SpaceURL.LOCATORS);
        String spaceURLHosts = null;
        //ONLY if Jini:// protocol but not multicast *
        if (spaceURL.getHost() != null && spaceURL.isJiniProtocol())
            spaceURLHosts = spaceURL.getHost();

        //handle double quote provided by linux when -Dcom.gs.jini_lus.locators=""
        if (sysPropLocators != null && sysPropLocators.startsWith("\"") && sysPropLocators.endsWith("\"")) {
            sysPropLocators = sysPropLocators.substring(1, sysPropLocators.length() - 1);
        }

        String fullLocatorsStr = sysPropLocators
                + ',' + xpathLocators + ',' + spaceURLLocators + "," + spaceURLHosts;
        String[] fullLocatorsArray = fullLocatorsStr.trim().split(",");

        HashSet<String> mergedLocatorsSet = new HashSet<String>();
        String mergedLocatorsStr = "";
        for (int i = 0; i < fullLocatorsArray.length; i++) {
            if (!fullLocatorsArray[i].equalsIgnoreCase("null") &&
                    !mergedLocatorsSet.contains(fullLocatorsArray[i])) {
                mergedLocatorsSet.add(fullLocatorsArray[i]);
                if (mergedLocatorsStr.equalsIgnoreCase("")) {
                    mergedLocatorsStr += fullLocatorsArray[i];
                } else {
                    mergedLocatorsStr += ',';
                    mergedLocatorsStr += fullLocatorsArray[i];
                }
            }
        }

        if (!JSpaceUtilities.isEmpty(mergedLocatorsStr)) {
            //added by Evgeny on 14.10.2007 in order to fix GS-3208
            //check if locator should be set
            //it should be set if not Jini protocol used OR
            //if Jini protocol used so any of: Xpath locator, sys prop locators ,
            //space url locator should not be null ( empty )
            boolean isLocatorsSet =
                    (spaceURL.isJiniProtocol() &&
                            (!JSpaceUtilities.isEmpty(sysPropLocators, true) ||
                                    !JSpaceUtilities.isEmpty(xpathLocators, true) ||
                                    !JSpaceUtilities.isEmpty(spaceURLLocators, true))) ||
                            !spaceURL.isJiniProtocol();

            if (isLocatorsSet) {
                customProperties.setProperty(XPathProperties.CONTAINER_JINI_LUS_UNICAST_HOSTS, mergedLocatorsStr);
                spaceURL.setProperty(SpaceURL.LOCATORS, mergedLocatorsStr);

                if (_logger.isDebugEnabled()) {
                    _logger.debug("SpaceURL locators attribute, <"
                            + XPathProperties.CONTAINER_JINI_LUS_UNICAST_HOSTS + "> XPATH were set to: "
                            + mergedLocatorsStr);
                }

                // enabling the Jini Unicast Discovery even if its disabled (by
                // default) in the container configuration since we assume that
                //if the user set locators it means he wants to have the unicast discovery enabled
                customProperties.setProperty(XPathProperties.CONTAINER_JINI_LUS_UNICAST_ENABLED, "true");
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Enabled the Jini Unicast Discovery flag");
                }
            }
        }//locators are defined
    }

    /**
     * Possible inputs for the Jini groups can be any combination of the following: 1. SpaceURL
     * groups attribute e.g. {@link com.j_spaces.core.client.SpaceURL#GROUPS} 2. properties XPATH
     * key e.g. {@link com.j_spaces.kernel.XPathProperties#CONTAINER_JINI_LUS_GROUPS} 3. System
     * property key e.g. {@link com.j_spaces.kernel.SystemProperties#JINI_LUS_GROUPS} In general we
     * take all the values from these inputs and merge them into one groups list.
     */
    static void handleGroups(SpaceURL spaceURL) {
        Properties customProperties = spaceURL.getCustomProperties();
        String customPropsGroups = customProperties.getProperty(XPathProperties.CONTAINER_JINI_LUS_GROUPS);

        String[] customGroups = null;
        if (!JSpaceUtilities.isEmpty(customPropsGroups, true)) {
            customGroups = StringUtils.tokenizeToStringArray(customPropsGroups, ",");
        }

        String sysPropGroups = SystemInfo.singleton().lookup().groups();
        if (!JSpaceUtilities.isEmpty(sysPropGroups))
            sysPropGroups = JSpaceUtilities.removeInvertedCommas(sysPropGroups);

        String groups = spaceURL.getProperty(SpaceURL.GROUPS);
        if (!JSpaceUtilities.isEmpty(groups)) {
            //GERSHON: APP-404 Fix 20/08/06
            //If the sys prop is already set we append to it the groups passed in SpaceURL &groups= attr
            //IF its not alreay there! and then re-set the sys prop and SpaceURL.
            String mergedGroups = groups;
            if (!JSpaceUtilities.isEmpty(sysPropGroups)) {
                //sys prop groups
                String[] sysGroups = JSpaceUtilities.parseLookupGroups(sysPropGroups);
                //space url groups
                String[] urlGroups = JSpaceUtilities.parseLookupGroups(groups);
                HashSet<String> urlGroupsSet = new HashSet<String>();
                if (urlGroups != null) {
                    for (int i = 0; i < urlGroups.length; i++) {
                        urlGroupsSet.add(urlGroups[i]);
                    }
                }
                //GERSHON: APP-549 Fix 19/10/06
                //When lookup group name defined as "all" NPE thrown in SpaceURLParser class
                if (sysGroups != null) {
                    final String defaultGroups = SystemInfo.singleton().lookup().defaultGroups();
                    for (int j = 0; j < sysGroups.length; j++) {
                        if (!urlGroupsSet.contains(sysGroups[j])) {
                            if (!sysGroups[j].equals(defaultGroups) || !StringUtils.hasLength(mergedGroups))
                                mergedGroups = mergedGroups + ',' + sysGroups[j];
                        }
                    }
                }
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Found [ -D" + SystemProperties.JINI_LUS_GROUPS
                            + "=" + sysPropGroups + " ] property - merging with space url attribute to: " +
                            mergedGroups);
                }
            }
            //merge with already defined in custom properties groups
            if (customGroups != null) {
                for (String prop : customGroups) {
                    if (!contains(mergedGroups, prop)) {
                        mergedGroups = mergedGroups + "," + prop;
                    }
                }
            }

            spaceURL.setProperty(SpaceURL.GROUPS,
                    JSpaceUtilities.removeInvertedCommas(mergedGroups));

            customProperties.setProperty(XPathProperties.CONTAINER_JINI_LUS_GROUPS,
                    JSpaceUtilities.removeInvertedCommas(mergedGroups));

        }
        //if no groups are defined in SpaceURL but we have SystemProperties.JINI_LUS_GROUPS set,
        //then we add &groups= to SpaceURL.
        else {
            if (JSpaceUtilities.isEmpty(sysPropGroups)) {
                sysPropGroups = SystemInfo.singleton().lookup().defaultGroups();
                System.setProperty(Constants.LookupManager.LOOKUP_GROUP_PROP,
                        sysPropGroups);
            }

            String mergedGroups = sysPropGroups;

            //merge with already defined in custom properties groups
            if (customGroups != null) {
                for (String prop : customGroups) {
                    if (!contains(mergedGroups, prop)) {
                        mergedGroups = mergedGroups + "," + prop;
                    }
                }
            }


            spaceURL.setProperty(SpaceURL.GROUPS, mergedGroups);
            customProperties.setProperty(XPathProperties.CONTAINER_JINI_LUS_GROUPS,
                    JSpaceUtilities.removeInvertedCommas(mergedGroups));

            if (_logger.isDebugEnabled()) {
                _logger.debug("Found [ -D" + SystemProperties.JINI_LUS_GROUPS + "=" + sysPropGroups
                        + " ] property. SpaceURL will be appended with the attribute groups="
                        + mergedGroups);
            }
        }// if no groups are defined in SpaceURL
    }

    /**
     * Return true if find ",member," | ",member$" | "^member," | "^member$"
     *
     * @param group  A string that describe a group of locator in the form "loc1,loc2,...,locn"
     * @param member A locator.
     * @return true if the group already contains the member as a locator.
     */
    private static boolean contains(String group, String member) {
        String quote = Pattern.quote(member);
        Pattern pattern = Pattern.compile("," + quote + "$|^" + quote + ",|^" + quote + "$" + "|," + quote + ",");
        Matcher matcher = pattern.matcher(group);
        return matcher.find();
    }

    /**
     * If client has requested to load a properties file (or passed a Properties java object), we
     * attempt to load the properties file from <GS dir>/config (if <GS dir> is in the classpath) or
     * from an http url. if it is found, we load it to the system, set these attributes to the
     * SpaceURL properties data structure. Any system property set overwrites any other settings,
     * then the Properties file (or user defined custom Properties object which is passed) is parsed
     * its properties are injected to the system and later used to override the
     * space/container/cluster etc. configurations, as well as other VM etc. system properties.
     *
     * The following types of properties are supported as part of the properties object/file:
     * ===================================================================================== 1.
     * space/container configuration - the prop key is the XPath of the element as it set in the
     * space/container schema xml file. e.g. For space config: space-config.lrmi-stub-handler.protocol-name=rmi
     * e.g. For container config: com.j_spaces.core.container.directory_services.jini_lus.enabled=false
     *
     * 2. cluster configuration - the prop key is the system property set into the ${elementname} in
     * the cluster schema xml file. 3. System properties - the key must start with -D, and indicates
     * the system to set that key/value as a system property. 4. SpaceURL attributes - the key must
     * start with gs.space.url.arg.<atribute name> e.g. gs.space.url.arg.total_members=111 or
     * gs.space.url.arg_line=schema=persistent&cluster_schema=async_replicated&total_members=3&id=2&nowritelease=true&fifo=true&create
     * The space url args should be all in lower case. Note, one can still use the ${xxx} setting a
     * system property in any scenario, but that will be overwritten if option number 1 is used.
     *
     * NOTE If we *.properties file AND custom properties object: We copy the properties key/values
     * found in the *.properties file into the custom properties object --> and overwriting the
     * existing values is exist!
     *
     * @return customProperties
     * @throws ConfigurationException if the specified resource could not be loaded
     */
    static Properties findAndSetCustomProperties(String propertiesName, SpaceURL spaceURL)
            throws IOException, ConfigurationException {
        Properties customProperties = spaceURL.getCustomProperties();
        if (!JSpaceUtilities.isEmpty(propertiesName)) {
            Properties customPropertiesFile = ResourceLoader.findCustomPropertiesObj(propertiesName);

            //Copy the properties key/values found in the *.properties file
            //into the custom properties object --> and overwriting the existing values is exist!
            customProperties.putAll(customPropertiesFile);
        }

        if (customProperties != null) {
            /** first sync with all the system properties and set them to the customProperties. **/

            /** now we iterate on all the customProperties elements and check which are SpaceURL attributes, which
             are system properties and which are space/container/cluster xpath properties. **/
            for (Enumeration keys = customProperties.keys(); keys.hasMoreElements(); ) {
                String propKey = (String) keys.nextElement();
                int spaceurlArgIndx = propKey.lastIndexOf(SpaceURL.PROPERTIES_SPACE_URL_ARG);
                int spaceurlArgLineIndx = propKey.lastIndexOf(SpaceURL.PROPERTIES_SPACE_URL_ARGLINE);
                int systemPropArg = propKey.lastIndexOf("-D");
                if (systemPropArg != -1) {
                    String systemPropKey = propKey.substring(2);

                    String propValue = customProperties.getProperty(propKey);
                    if (!JSpaceUtilities.isEmpty(propValue))
                        propValue = JSpaceUtilities.removeInvertedCommas(propValue);

                    System.setProperty(systemPropKey, propValue);
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Key - ["
                                + systemPropKey
                                + "] Value - ["
                                + propValue
                                + "] was set as a System Property from Properties object.");
                    }
                } else if (spaceurlArgIndx != -1 && spaceurlArgLineIndx == -1) {
                    String spaceurlArg = propKey.substring(propKey.lastIndexOf('.') + 1);
                    if (SpaceURL.isUrlAttribute(spaceurlArg))//means we deal with a spaceurl attribute
                    {
                        String propValue = customProperties.getProperty(propKey);
                        if (!JSpaceUtilities.isEmpty(propValue))
                            propValue = JSpaceUtilities.removeInvertedCommas(propValue);
                        //For boolean attributes - empty assignment translates to true
                        if (propValue.trim().equals("") && SpaceURL.isBooleanUrlAttribute(spaceurlArg))
                            propValue = "true";//in case passed create, destroy, ignoreValidation without value, we set its value to "true"
                        spaceURL.setProperty(spaceurlArg.toLowerCase(),
                                propValue);
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("SpaceURL attribute - ["
                                    + spaceurlArg.toLowerCase()
                                    + "] Value - ["
                                    + propValue
                                    + "] was added to SpaceURL from Properties object.");
                        }
                    } else
                    //throw msg that this space url key is not valid...
                    {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("A non valid SpaceURL attribute - ["
                                    + spaceurlArg.toLowerCase()
                                    + "] Value - ["
                                    + customProperties.getProperty(propKey)
                                    + "] was loaded from Properties object.");
                        }
                    }
                } else if (spaceurlArgLineIndx != -1) {
                    // parse URL attributes using & separator between every attribute
                    // initialize url for parsing
                    String spaceAttribArgLine = customProperties.getProperty(propKey);
                    spaceAttribArgLine = JSpaceUtilities.removeInvertedCommas(spaceAttribArgLine);
                    StringTokenizer st = new StringTokenizer(spaceAttribArgLine, "&");
                    if (propKey.indexOf("&") != -1)
                        st = new StringTokenizer(st.nextToken(), "&");

                    String attrName = null;
                    String attrValue = null;
                    for (int i = 0; st.hasMoreTokens(); i++) {
                        try {
                            // parse attribute name and attribute value using = separator
                            StringTokenizer stp = new StringTokenizer(st.nextToken(), "=");
                            attrName = stp.nextToken().toLowerCase();
                            if (SpaceURL.isUrlAttribute(attrName))//means we
                            // deal with a spaceurl attribute
                            {
                                attrValue = stp.nextToken();
                                if (attrValue.trim().equals("") && SpaceURL.isBooleanUrlAttribute(attrName))
                                    attrValue = "true";//in case passed create, destroy, ignoreValidation without value, we set its value to "true"
                                spaceURL.setProperty(attrName.toLowerCase(),
                                        attrValue);
                            }
                        } catch (NoSuchElementException ex) {
                            if (SpaceURL.isBooleanUrlAttribute(attrName)) { // attribute name without attr value, for
                                // example: <spaceName>?create&clustered
                                spaceURL.setProperty(attrName.toLowerCase(),
                                        "true");
                            }
                            attrValue = null;

                        }
                        if (_logger.isDebugEnabled()) {
                            if (attrValue != null) {
                                _logger.debug("SpaceURL attribute - ["
                                        + attrName.toLowerCase()
                                        + "] Value - ["
                                        + attrValue
                                        + "] was added to SpaceURL from Properties object.");
                            } else {
                                _logger.debug("SpaceURL attribute - ["
                                        + attrName.toLowerCase()
                                        + "] Value - [true] was added to SpaceURL from Properties object.");
                            }
                        }
                    } // for ...
                }//else if
            }//for
        }
        return customProperties;
    }

    public static String getRMIDurl(String rmidURL) {
        return rmidURL.indexOf(':') == -1 ? rmidURL + ':' + DEFAULT_RMID_PORT : rmidURL;
    }

    // print usage and exit
    public static void printUsage()//TODO add the new cluster semi dynamic examples. The dot notations.
    {
        if (_logger.isInfoEnabled()) {
            _logger.info("Use: Protocol://[host]:[port]/[container_name]/[space_name]?[query_string]"
                    + "\nProtocol: [ RMI | JINI | JAVA (for embedded instance) | WS ]"
                    + "\n\nExamples of Space Urls:"
                    + "\n/./mySpace?schema=cache&properties=gs (which uses default gs.properties configuration file)"
                    + "\nor java://localhost:10098/containerNameor "
                    + "\nor java://localhost:10098/containerName/mySpace"
                    + "\nor /./mySpace (which translates to java://localhost:10098/containerName/mySpace?schema=default)"
                    + "\nor /./mySpace?schema=cache (which translates to java://localhost:10098/containerName/mySpace?schema=cache)");

            _logger.info("\nSetting up 4 nodes cluster with a partitioned cache with backup instances will be done using the following space url:"
                    + "\nFor Member 1: /./mySpace?schema=cache&cluster_schema=partitioned&total_members=4,2&id=1"
                    + "\nFor Member 1 backup 1: /./mySpace?schema=cache&cluster_schema=partitioned&total_members=4,2&id=2&backup_id=1");

            _logger.info("\nSetting up 4 nodes cluster with an async replicated policy will be done using the following space url:"
                    + "\nFor Member 1: /./mySpace?schema=cache&cluster_schema=async_replicated&total_members=4&id=1"
                    + "\nFor Member 2: /./mySpace?schema=cache&cluster_schema=async_replicated&total_members=4&id=2");

            _logger.info("\nExample of Multicast lookup using Jini: jini:/*/container name/space name"
                            + "\njini:/*/*/space name"
                            + "\njini:/*/container name"
                            + "\n\nExample of Unicast lookup using Jini: jini:/myhost/container name/space name"
                            + "\njini:/myhost/*/space name"
                            + "\njini:/myhost/container name"
            /*+ SpaceURL.printSpaceURLQueryAtributes()*/);
        }
    }
}
