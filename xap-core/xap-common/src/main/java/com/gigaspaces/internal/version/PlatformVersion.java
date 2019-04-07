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

package com.gigaspaces.internal.version;

import com.gigaspaces.logger.LoggerSystemInfo;
import com.gigaspaces.start.ProductType;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.ErrorManager;

@com.gigaspaces.api.InternalApi
public class PlatformVersion {
    private static final PlatformVersion instance = new PlatformVersion(getVersionPropertiesFromFile("com/gigaspaces/internal/version/PlatformVersion.properties"));

    private final String version;
    private final String milestone;
    private final String buildNumber;
    private final String id;
    private final String officialVersion;
    private final byte majorVersion;
    private final byte minorVersion;
    private final byte spVersion;
    private final int shortBuildNumber;
    private final int subBuildNumber;
    private final String revision;
    private final String productHelpUrl;
    private final ProductType productType;

    public PlatformVersion(Properties properties) {
        version = properties.getProperty("xap.version", "14.0.0");
        milestone = properties.getProperty("xap.milestone", "m1");
        buildNumber = properties.getProperty("xap.build.number", "19901-10");
        revision = properties.getProperty("xap.git.sha", "unspecified");

        this.id = String.format("%s-%s-%s", version, milestone, buildNumber);
        this.productType = isInsightEdge() ? ProductType.InsightEdge : ProductType.XAP;
        officialVersion = String.format("GigaSpaces %s %s %s (build %s, revision %s)",
                productType.name(), version, milestone.toUpperCase(), buildNumber, revision);

        String[] versionTokens = version.split("\\.");
        majorVersion = Byte.parseByte(versionTokens[0]);
        minorVersion = Byte.parseByte(versionTokens[1]);
        spVersion = Byte.parseByte(versionTokens[2]);

        final String[] buildNumberTokens = buildNumber.split("-");
        shortBuildNumber = Integer.parseInt(buildNumberTokens[0]);
        subBuildNumber = buildNumberTokens.length == 1 ? 0 : Integer.parseInt(buildNumberTokens[1]);

        productHelpUrl = "https://docs.gigaspaces.com/" + majorVersion + "." + minorVersion;
    }

    private static boolean isInsightEdge() {
        return new File(LoggerSystemInfo.xapHome + File.separator + "insightedge").exists();
    }

    public static PlatformVersion getInstance() {
        return instance;
    }

    public static void main(String args[]) {
        System.out.println(PlatformVersion.getOfficialVersion());
    }

    private static Properties getVersionPropertiesFromFile(String path) {
        Properties properties = new Properties();
        try {
            InputStream inputStream = PlatformVersion.class.getClassLoader().getResourceAsStream(path);
            if (inputStream != null) {
                properties.load(inputStream);
                inputStream.close();
            }
        } catch (Throwable t) {
            ErrorManager errorManager = new ErrorManager();
            errorManager.error("Failed to load version properties from " + path, new Exception(t), ErrorManager.OPEN_FAILURE);
        }
        return properties;
    }

    public ProductType getProductType() {
        return productType;
    }

    public byte getMajorVersion() {
        return majorVersion;
    }

    public byte getMinorVersion() {
        return minorVersion;
    }

    byte getServicePackVersion() {
        return spVersion;
    }

    int getShortBuildNumber() {
        return shortBuildNumber;
    }

    int getSubBuildNumber() {
        return subBuildNumber;
    }

    public String getId() {
        return id;
    }

    /**
     * @return e.g. 6191
     */
    public String getBuildNumber() {
        return buildNumber;
    }

    /**
     * @return GigaSpaces XAP 8.0.6 GA (build 6191)
     */
    public static String getOfficialVersion() {
        return instance.officialVersion;
    }

    /**
     * @return e.g. 8.0.6
     */
    public static String getVersion() {
        return instance.version;
    }

    /**
     * Gets the git revision (sha/tag)
     * @return
     */
    public static String getRevision() {
        return instance.revision;
    }

    public static String getProductHelpUrl() {
        return instance.productHelpUrl;
    }
}
