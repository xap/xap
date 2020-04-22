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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.internal.classloader.AbstractClassRepository;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.j_spaces.kernel.ResourceLoader;

import org.w3c.dom.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceTypeInfoRepository extends AbstractClassRepository<SpaceTypeInfo> {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_METADATA_POJO);
    private static final String GS_MAPPING = "/config/mapping";
    private static final SpaceTypeInfoRepository _globalRepository = new SpaceTypeInfoRepository();

    public SpaceTypeInfoRepository() {
        this.initialize();
    }

    public static SpaceTypeInfoRepository getGlobalRepository() {
        return _globalRepository;
    }

    public static SpaceTypeInfo getTypeInfo(Class<?> type) {
        return _globalRepository.getByType(type);
    }

    private void initialize() {
        // Loading all gs xml files
        try {
            loadFolder(GS_MAPPING);
        }
        //catch (FileNotFoundException e)
        //{
        //    // no config/mapping, no need to log a warning for it
        //}
        catch (Throwable e) {
            if (_logger.isWarnEnabled()) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Failed to parse *.gs.xml files from folder '" + GS_MAPPING + "': ", e);
                else
                    _logger.warn("Failed to parse *.gs.xml files from folder '" + GS_MAPPING +
                            "' for more information set logger '" + _logger.getName() + "' level to FINE.");
            }
        }
    }

    @Override
    protected SpaceTypeInfo create(Class<?> type, SpaceTypeInfo superTypeInfo, Object context) {
        // Create type info:
        SpaceTypeInfo typeInfo = new SpaceTypeInfo(type, superTypeInfo, (Map<String, Node>) context);

        if (_logger.isDebugEnabled()) {
            if (_logger.isDebugEnabled())
                _logger.debug("Type metadata created and cached: [" + type.getName() + "].\n" + typeInfo.getFullDescription());
            else
                _logger.debug("Type metadata created and cached: [" + type.getName() + "].");
        }

        // Return result:
        return typeInfo;
    }

    public void loadFolder(String path) {
        path = path.trim();
        URL url = ResourceLoader.getResourceURL(path);
        if (url == null)
            return;
        //throw new FileNotFoundException(path + " cannot be resolved to URL because it does not exist.");

        File file = new File(URLDecoder.decode(url.getFile()));

        if (_logger.isDebugEnabled())
            _logger.debug("Scanning for type metadata info at [" + file.getAbsolutePath() + "].");

        Map<String, Node> xmlMap = new HashMap<String, Node>();
        loadPath(file, xmlMap);

        final String[] typeNames = xmlMap.keySet().toArray(new String[xmlMap.size()]);

        if (_logger.isDebugEnabled())
            _logger.debug("Found " + typeNames.length + " type metadata specifications while scanning [" + file.getAbsolutePath() + "].");

        for (String typeName : typeNames) {
            try {
                SpaceTypeInfo typeInfo = super.getByName(typeName, xmlMap);
                if (typeInfo == null && _logger.isWarnEnabled())
                    _logger.warn("Skipped gs.xml for type [" + typeName + "] because type could not be loaded.");
            } catch (Throwable e) {
                if (_logger.isWarnEnabled()) {
                    if (_logger.isDebugEnabled())
                        _logger.warn("Failed to load type info for type [" + typeName + "].", e);
                    else
                        _logger.warn("Failed to load type info for type [" + typeName + "]: " + e.getMessage());
                }
            }
        }

        if (_logger.isDebugEnabled())
            _logger.debug("Finished loading types metadata from [" + file.getAbsolutePath() + "].");
    }

    private static void loadPath(File file, Map<String, Node> xmlMap) {
        if (file.isDirectory()) {
            File[] subFiles = BootIOUtils.listFiles(file);
            for (int i = 0; i < subFiles.length; i++)
                loadPath(subFiles[i], xmlMap);
        } else if (file.getName().endsWith(".gs.xml")) {
            if (_logger.isDebugEnabled())
                _logger.debug("Scanning for type metadata info at [{0}].", file.getAbsolutePath());

            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream(file);
                GsXmlParser.parseGsXml(inputStream, xmlMap, file.getAbsolutePath());
            } catch (Exception e) {
                if (_logger.isErrorEnabled())
                    _logger.error("Error while scanning for type metadata info at [" + file.getAbsolutePath() + "].", e);
            } finally {
                try {
                    if (inputStream != null)
                        inputStream.close();
                } catch (IOException e) {
                    if (_logger.isWarnEnabled())
                        _logger.warn("Could not close input stream for file [" + file.getAbsolutePath() + "].", e);
                }
            }
        }
    }

    public static Node loadFile(String fileName, String typeName) {
        InputStream inputStream = ResourceLoader.getResourceStream(fileName);
        if (inputStream == null)
            return null;

        try {
            // Retrieve the requested pojo description
            Map<String, Node> xmlMap = new HashMap<String, Node>();
            GsXmlParser.parseGsXml(inputStream, xmlMap, fileName);
            for (Entry<String, Node> entry : xmlMap.entrySet())
                if (entry.getKey().equals(typeName))
                    return entry.getValue();

            return null;
        } catch (Exception e) {
            throw new ConversionException("Could not parse the mapping document in input stream", e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                if (_logger.isWarnEnabled())
                    _logger.warn("Could not close input stream", e);
            }
        }
    }
}
