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

import com.j_spaces.core.cluster.ClusterXML;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.lookup.entry.State;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
@com.gigaspaces.api.InternalApi
public class SpaceURLValidator {

    //logger
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACE_URL);

    private static final HashSet<String> urlElements = new HashSet<String>();
    private static final String[] urlElementsNames =
            {
                    SpaceURL.CLUSTER_SCHEMA.toLowerCase(),
                    SpaceURL.CLUSTER_TOTAL_MEMBERS.toLowerCase(),
                    SpaceURL.CLUSTER_MEMBER_ID.toLowerCase(),
                    SpaceURL.CLUSTER_BACKUP_ID.toLowerCase(),
                    SpaceURL.CLUSTER_GROUP.toLowerCase(),
                    SpaceURL.CLUSTER_NAME.toLowerCase(),
                    SpaceURL.SCHEMA_NAME.toLowerCase(),
                    SpaceURL.GROUPS.toLowerCase(),
                    SpaceURL.FIFO_MODE.toLowerCase(),
                    SpaceURL.LOCAL_CACHE_UPDATE_MODE.toLowerCase(),
                    SpaceURL.LOCAL_CACHE_STORAGE_TYPE.toLowerCase(),
                    SpaceURL.VERSIONED.toLowerCase(),
                    SpaceURL.USE_LOCAL_CACHE.toLowerCase(),
                    SpaceURL.VIEWS.toLowerCase(),
                    SpaceURL.TIMEOUT.toLowerCase(),
                    SpaceURL.SECURITY_MANAGER.toLowerCase(),
                    //SpaceURL.NO_WRITE_LEASE.toLowerCase(),
                    SpaceURL.MIRROR.toLowerCase(),
                    SpaceURL.CREATE.toLowerCase(),
                    SpaceURL.PROPERTIES_FILE_NAME.toLowerCase(),
                    SpaceURL.IGNORE_VALIDATION.toLowerCase(),
                    SpaceURL.STATE.toLowerCase(),
                    SpaceURL.SPACE.toLowerCase(),
                    SpaceURL.MEMBER_NAME.toLowerCase(),
                    SpaceURL.CONTAINER.toLowerCase(),
                    SpaceURL.PROTOCOL_NAME.toLowerCase(),
                    SpaceURL.HOST_NAME.toLowerCase(),
                    SpaceURL.LOCATORS.toLowerCase(),
                    SpaceURL.URL_NAME.toLowerCase(),
                    SpaceURL.MACHINE_HOSTNAME.toLowerCase(),
                    SpaceURL.SECURED.toLowerCase()
            };

    static {
        urlElements.addAll(Arrays.asList(urlElementsNames));
    }

    /**
     * @return String array which its first element is the number of primary members. If there is a
     * second element in this array, it holds the backup members per each primary.
     */
    public static String[] getClusterMembers(String totalMembers) {
        // check if the ?total_members contains also the number Of Backup Members Per Primary
        //(which is the next value after the ",")
        return JSpaceUtilities.splitString(totalMembers.trim(), ',');//we do not except spaces in the _totalMembers
    }

    /**
     * Validates various SpaceURL combinations with the cluster_schema, total_members is and
     * backup_id arguments.
     *
     * @throws MalformedURLException if fails in validation test
     */
    public static void validateClusterSchemaAttributes(SpaceURL spaceURL)
            throws MalformedURLException {

        //check total cluster members number, must be more than one
        //that in the case of cluster without backups
        if (spaceURL.containsKey(SpaceURL.CLUSTER_TOTAL_MEMBERS)) {
            String[] clusterTotalMembers =
                    getClusterMembers(spaceURL.getProperty(SpaceURL.CLUSTER_TOTAL_MEMBERS));
            if (clusterTotalMembers.length == 1 &&
                    Integer.parseInt(clusterTotalMembers[0]) <= 1) {
                throw new SpaceURLValidationException("The <" + SpaceURL.CLUSTER_TOTAL_MEMBERS +
                        "> attribute must be larger than 1" +
                        " in the space URL: " + spaceURL);
            }
        }

        //if we have cluster_schema but not total_members, throw exception
        if (spaceURL.containsKey(SpaceURL.CLUSTER_SCHEMA) &&
                !(spaceURL.containsKey(SpaceURL.CLUSTER_TOTAL_MEMBERS))) {
            throw new SpaceURLValidationException("The <" + SpaceURL.CLUSTER_SCHEMA +
                    "> attribute must be used together with the <" +
                    SpaceURL.CLUSTER_TOTAL_MEMBERS + "> attribute " +
                    "in the space URL: " + spaceURL);
        }

        // if we have total_members but NO cluster_schema, throw exception
        if (spaceURL.containsKey(SpaceURL.CLUSTER_TOTAL_MEMBERS) &&
                !(spaceURL.containsKey(SpaceURL.CLUSTER_SCHEMA))) {
            throw new SpaceURLValidationException("The <" + SpaceURL.CLUSTER_TOTAL_MEMBERS +
                    "> attribute must be used together with the <" +
                    SpaceURL.CLUSTER_SCHEMA + "> attribute " +
                    "in the space URL: " + spaceURL);
        }

        // if we have cluster_schema and total_members NO id we throw exception
        if (spaceURL.containsKey(SpaceURL.CLUSTER_SCHEMA) &&
                spaceURL.containsKey(SpaceURL.CLUSTER_TOTAL_MEMBERS)) {
            if (!spaceURL.containsKey(SpaceURL.CLUSTER_MEMBER_ID)) {
                throw new SpaceURLValidationException("The <" + SpaceURL.CLUSTER_SCHEMA + "=" + spaceURL.getProperty(SpaceURL.CLUSTER_SCHEMA) +
                        "> attribute must be used together with the <" +
                        SpaceURL.CLUSTER_MEMBER_ID + "> attribute in the space URL: " + spaceURL);
            }

            String clusterSchema = spaceURL.getProperty(SpaceURL.CLUSTER_SCHEMA);
            String totalMembers = spaceURL.getProperty(SpaceURL.CLUSTER_TOTAL_MEMBERS);
            String[] totalMembersArray = getClusterMembers(totalMembers);

            if (totalMembersArray.length > 1) {
                if (ClusterXML.supportsBackup(clusterSchema)) {
                    try {
                        int pNum = Integer.valueOf(totalMembersArray[0]).intValue();
                        int bNum = Integer.valueOf(totalMembersArray[1]).intValue();
                        if (pNum < 1 || bNum < 0) {
                            throw new SpaceURLValidationException("The <" + SpaceURL.CLUSTER_SCHEMA + "=" + clusterSchema +
                                    "> attribute must be used together with total_members attribute in the following format " +
                                    "total_members={number of primary instances, number of backup instances per primary} " +
                                    "in the space URL: " + spaceURL
                                    + ".\nThe number of primary instances must be greater than 0 and "
                                    + ".\nthe number of number of backup instances per primary must not be less than 0.");
                        }
                    } catch (NumberFormatException e) {
                        throw new SpaceURLValidationException("The <"
                                + SpaceURL.CLUSTER_SCHEMA
                                + "="
                                + clusterSchema
                                + "> attribute must be used together with the total_members attribute in the following format "
                                + "total_members={number of primary instances, number of backup instances per primary} "
                                + "in the space URL: " + spaceURL
                                + ".\nThe number of primary instances must be greater than 0 and "
                                + ".\nthe number of number of backup instances per primary must not be less than 0.");
                    }
                    // CHECK IF POSITIVE OR NOT ETC
                }
                // CHECK DO WE ALLOW TO HAVE {NUMBER of primary instances,
                // NUMBER of backup instances per primary} in other schemas as well besides the
                //ClusterXML.CLUSTER_SCHEMA_NAME_PARTITIONED_SYNC2BACKUP)
                //ClusterXML.CLUSTER_SCHEMA_NAME_PRIMARY_BACKUP
                else {
                    int bNum = Integer.valueOf(totalMembersArray[1]).intValue();
                    if (bNum > 0)
                        throw new SpaceURLValidationException("The <"
                                + SpaceURL.CLUSTER_SCHEMA
                                + "="
                                + clusterSchema
                                + "> attribute must be used together with the total_members attribute in the following format "
                                + "total_members={number of primary instances} "
                                + "in the space URL: " + spaceURL
                                + ".\nOnly cluster_schema which ends with " + ClusterXML.CLUSTER_SCHEMA_NAME_PARTITIONED
                                + " or " + ClusterXML.CLUSTER_SCHEMA_NAME_PRIMARY_BACKUP + " \ncan have total_member attribute in the following format "
                                + "total_members={number of primary instances, number of backup instances per primary}.");

                }
            }
        }

        String updateModeStr = spaceURL.getProperty(SpaceURL.LOCAL_CACHE_UPDATE_MODE);
        if (updateModeStr != null) {
            int updateMode = Integer.valueOf(updateModeStr);
            if (updateMode != SpaceURL.UPDATE_MODE_PULL && updateMode != SpaceURL.UPDATE_MODE_PUSH) {
                throw new SpaceURLValidationException("The <" + SpaceURL.LOCAL_CACHE_UPDATE_MODE +
                        "> attribute must be equal to " + SpaceURL.UPDATE_MODE_PULL + " or " +
                        SpaceURL.UPDATE_MODE_PUSH + " in the space URL: " + spaceURL);
            }
        }

        State state = (State) spaceURL.getLookupAttribute(SpaceURL.STATE);
        if (state != null) {
            if (!state.state.equalsIgnoreCase(SpaceURL.STATE_STARTED) &&
                    !state.state.equalsIgnoreCase(SpaceURL.STATE_STOPPED)) {
                throw new SpaceURLValidationException("The <" + SpaceURL.STATE +
                        "> attribute must be equal to " + SpaceURL.STATE_STARTED + " or " +
                        SpaceURL.STATE_STOPPED + " in the space URL: " + spaceURL);
            }
        }
    }


    /**
     * The method is used for valid the space url before finding it from space
     *
     * @param spaceURL space url object
     * @throws SpaceURLValidationException thrown if something went wrong
     */
    public static void validate(SpaceURL spaceURL) throws Exception {
        if (_logger.isDebugEnabled()) {
            _logger.debug("attributesSpaceUrl  =  " + spaceURL);
        }
        try {
            validateURL(spaceURL);
        } catch (SpaceURLValidationException e) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Exception while validating space URL: " + e.getMessage() +
                        "\nSpace URL: " + spaceURL);
            }
            throw e;
        }
    }

    /**
     * The method validates the space URL parameter names
     *
     * @param spaceURL the space URL
     */
    private static void validateURL(SpaceURL spaceURL) throws SpaceURLValidationException {
        Enumeration names = spaceURL.propertyNames();
        while (names.hasMoreElements()) {
            String property = names.nextElement().toString();
            if (_logger.isDebugEnabled())
                _logger.debug("Validating Space URL property: " + property + " = " + spaceURL.getProperty(property));
            if (!urlElements.contains(property.toLowerCase()))
                throw new SpaceURLValidationException("Invalid space url property - '" + property + "'");
        }
    }
}
