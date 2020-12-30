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

import com.gigaspaces.start.ProductType;
import com.gigaspaces.start.SystemLocations;

import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.logging.ErrorManager;

@com.gigaspaces.api.InternalApi
public class PlatformVersion {
    private static final PlatformVersion instance = new PlatformVersion(getVersionPropertiesFromFile("com/gigaspaces/internal/version/PlatformVersion.properties"));

    private final String id;
    private final String version;
    private final String officialVersion;
    private final byte majorVersion;
    private final byte minorVersion;
    private final byte spVersion;
    private final String tag;
    private final Properties shas;
    private final Properties shasTimestamps;
    private final String revision;
    private final String productHelpUrl;
    private final ProductType productType;
    private final String patchId;
    private final int patchNumber;

    public PlatformVersion(Properties properties) {
        this.id = properties.getProperty("gs.build-name");
        this.version = extractPrefix(id, "-");
        this.productType = isInsightEdge() ? ProductType.InsightEdge : ProductType.XAP;
        this.officialVersion = "GigaSpaces " + productType + " " + id;
        this.tag = properties.getProperty("gs.git-tag");
        this.shas = extractPropertiesByPrefix(properties, "gs.git-sha.");
        this.shasTimestamps = extractPropertiesByPrefix(properties, "gs.git-ts.");
        this.revision = initRevision(tag, shas);

        String[] patchTokens = extractPatchTokens(this.id, this.version);
        this.patchId = patchTokens[0];
        this.patchNumber = Integer.parseInt(patchTokens[1]);

        String[] versionTokens = version.split("\\.");
        majorVersion = Byte.parseByte(versionTokens[0]);
        minorVersion = Byte.parseByte(versionTokens[1]);
        spVersion = Byte.parseByte(versionTokens[2]);

        productHelpUrl = "https://docs.gigaspaces.com/" + majorVersion + "." + minorVersion;
    }

    private static String extractPrefix(String s, String separator) {
        int pos = s.indexOf(separator);
        return pos == -1 ? s : s.substring(0, pos);
    }

    private static String[] extractPatchTokens(String id, String version) {
        String prefix = version + "-patch-";
        return id.startsWith(prefix) ? id.replace(prefix, "").split("-") : new String[] {"", "0"};
    }

    private static boolean isInsightEdge() {
        return Files.exists(SystemLocations.singleton().tools("jdbc"));
    }

    public static boolean isInsightEdgeAnalytics() {
        return Files.exists(SystemLocations.singleton().home("insightedge"));
    }

    private static String initRevision(String tag, Properties shas) {
        if (tag != null && !tag.equals("Unspecified"))
            return tag;
        return !shas.isEmpty() ? shas.toString() : "unknown";
    }

    private static Properties extractPropertiesByPrefix(Properties properties, String prefix) {
        Properties result = new Properties();
        properties.entrySet().stream()
                .filter(p -> p.getKey().toString().startsWith(prefix))
                .forEach(p -> result.put(p.getKey().toString().substring(prefix.length()), p.getValue()));
        return result;
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

    public static String getProductDownloadUrl(String productType, String productVersion){
        return "http://gigaspaces-releases-eu.s3.amazonaws.com/" + productType + "/" + extractPrefix(productVersion,"-") + "/" + getProductZipName(productType,productVersion);
    }

    private static String getProductZipName(String productType, String productVersion) {
        return "gigaspaces-" + productType + "-enterprise-" + productVersion + ".zip";
    }

    public static String getProductFolderName(String productType, String productVersion) {
        return "gigaspaces-" + productType + "-enterprise-" + productVersion;
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

    String getPatchId() {
        return patchId;
    }

    int getPatchNumber() {
        return patchNumber;
    }

    public String getId() {
        return id;
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

    public static Properties getShas() {
        return instance.shas;
    }

    public static Properties getShasTimestamps() {
        return instance.shasTimestamps;
    }
}
