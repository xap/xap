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

package org.openspaces.core.config.xmlparser;

import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageConfig;
import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class TieredStorageDefinitionsParser {

    private static final Logger logger = LoggerFactory.getLogger(TieredStorageDefinitionsParser.class);
    private static final String TABLES = "os-core:tables";
    private static final String TABLE = "os-core:table";

    public static void parseXml(Element tieredStorageElement, BeanDefinitionBuilder builder) {
        TieredStorageConfig tieredStorageConfig = new TieredStorageConfig();
        NodeList tableNodesList = tieredStorageElement.getElementsByTagName(TABLES);
        if (tableNodesList.getLength() > 0) {
            Map<String, TieredStorageTableConfig> tablesMap = new HashMap<>();
            Node tablesNode = tableNodesList.item(0);
            Element tablesElement = (Element) tablesNode;
            NodeList tables = tablesElement.getElementsByTagName(TABLE);
            for (int i = 0; i < tables.getLength(); i++) {
                Node tableNode = tables.item(i);
                TieredStorageTableConfig tableConfig = new TieredStorageTableConfig();
                NamedNodeMap attributes = tableNode.getAttributes();

                tableConfig.setName(getAttribute("name",attributes));
                tableConfig.setTimeColumn(getAttribute("time-column",attributes));
                tableConfig.setRetention(getDurationAttribute("retention", attributes));
                String sTransient = getAttribute("transient", attributes);
                if (sTransient != null){
                    tableConfig.setTransient(Boolean.parseBoolean(sTransient));
                }
                NodeList cacheRulesList = ((Element) tableNode).getElementsByTagName("os-core:cache-rule");
                if (cacheRulesList.getLength() > 0){
                    Node cacheRuleNode = cacheRulesList.item(0);
                    NamedNodeMap cacheRuleNodeAttributes = cacheRuleNode.getAttributes();
                    tableConfig.setCriteria(getAttribute("criteria",cacheRuleNodeAttributes));
                    tableConfig.setPeriod(getDurationAttribute("period", cacheRuleNodeAttributes));
                }
                else {
                    logger.debug("no cache-rule for table "+tableConfig.getName());
                }
                if (tablesMap.putIfAbsent(tableConfig.getName(), tableConfig) != null){
                    logger.debug("table "+tableConfig.getName() + " appears more than once");
                }
            }
            tieredStorageConfig.setTables(tablesMap);


        }
        else {
            logger.error("no tables entity found in tiered storage");
        }
        builder.addPropertyValue("tieredStorageConfig", tieredStorageConfig);

    }

    private static Duration getDurationAttribute(String attName, NamedNodeMap attributes){
        String attribute = getAttribute(attName, attributes);
        if (attribute != null){
            return Duration.parse(attribute);
        }
        return null;
    }

    private static String getAttribute(String attName, NamedNodeMap attributes){
        Node namedItem = attributes.getNamedItem(attName);
        if (namedItem != null){
            return namedItem.getNodeValue();
        }
        return null;
    }

}
