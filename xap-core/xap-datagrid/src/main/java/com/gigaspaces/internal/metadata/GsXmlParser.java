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

import com.gigaspaces.internal.io.XmlUtils;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Provides gs.xml parsing services.
 *
 * @author Niv Ingberg
 * @since 7.0.1
 */
public abstract class GsXmlParser {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_METADATA_POJO);

    private static final EntityResolver _entityResolver = new GsXmlEntityResolver();

    public static void parseGsXml(InputStream inputStream, Map<String, Node> map, String sourceDesc)
            throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilder documentBuilder = XmlUtils.getDocumentBuilder();
        documentBuilder.setEntityResolver(_entityResolver);
        Document xmlDocument = documentBuilder.parse(inputStream);
        parseGsXml(xmlDocument, map, sourceDesc);
    }

    private static void parseGsXml(Document xmlDocument, Map<String, Node> map, String sourceDesc) {
        Element xmlElement = xmlDocument.getDocumentElement();
        NodeList nodeList = xmlElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            String nodeName = node.getNodeName();
            if (nodeName.equals("class")) {
                String typeName = XmlUtils.getAttribute(node, "name");
                if (map.containsKey(typeName)) {
                    if (_logger.isWarnEnabled())
                        _logger.warn("Found duplicate metadata for type [" + typeName + "] while scanning [" + sourceDesc + "].");
                } else {
                    map.put(typeName, node);
                    if (_logger.isDebugEnabled())
                        _logger.debug("Found metadata for type [" + typeName + "] while scanning [" + sourceDesc + "].");
                }
            } else if (_logger.isWarnEnabled())
                _logger.warn("Unrecognized xml node: " + nodeName);
        }
    }

    private static class GsXmlEntityResolver implements EntityResolver {
        public InputSource resolveEntity(String publicID, String systemID)
                throws IOException, SAXException {
            // Handle references to online version of copyright.xml
            if (systemID.indexOf("gigaspaces-metadata.dtd") != -1)
                return (new InputSource(this.getClass().getClassLoader().getResourceAsStream("gigaspaces-metadata.dtd")));

            // In the default case, return null
            return null;
        }
    }
}
