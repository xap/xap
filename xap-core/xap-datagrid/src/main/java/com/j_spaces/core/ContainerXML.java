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

package com.j_spaces.core;

import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.kernel.JSpaceUtilities;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static com.j_spaces.core.Constants.Container.CONTAINER_CONFIG_FILE_SUFFIX;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class ContainerXML {

    // Container configuration XML tags
    public static final String TAG_CONTAINER = "container";
    public static final String TAG_CONTAINER_ADMIN_NAME = "admin-name";
    public static final String TAG_CONTAINER_ADMIN_PASSWORD = "admin-password";

    //logger
    final private static Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONFIG);

    /**
     * Set container administrator account
     */
    public static void setAdmin(boolean useSchema, String containerName,
                                String adminName, String adminPassword) throws Exception {
        setTags(useSchema,
                new String[]{TAG_CONTAINER_ADMIN_NAME, TAG_CONTAINER_ADMIN_PASSWORD},
                new String[]{adminName, adminPassword},
                TAG_CONTAINER, containerName);
    }

    private static Document getRootDocument(File xmlFile)
            throws ParserConfigurationException, SAXException, IOException {
        // Obtaining a org.w3c.dom.Document from XML
        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder domBuilder = domFactory.newDocumentBuilder();

        // get reference to Document object of container
        Document rootDocument = domBuilder.parse(xmlFile);
        JSpaceUtilities.normalize(rootDocument);

        return rootDocument;
    }

    private static String getContainerConfigPathName(String containerName) {
        StringBuilder result = new StringBuilder();
        result.append(SystemLocations.singleton().config());
        result.append(File.separator);
        result.append(containerName);
        result.append(CONTAINER_CONFIG_FILE_SUFFIX);
        return result.toString();
    }


    private static Node createTextNode(Document rootDoc, String tagName, String tagValue) {
        Element tag = rootDoc.createElement(tagName);
        Text tagText = rootDoc.createTextNode(tagValue);
        tag.appendChild(tagText);
        return tag;
    }

    /**
     * Every change in container the container xml file will be updating be this method. On every
     * update current file will be renamed to <%filename%>.old and the new one will be
     * <%filename%>.xml
     *
     * @Exception FileNotFoundException - if the file exists but is a directory rather than a
     * regular file, does not exist but cannot be created, or cannot be opened for any other
     * reason.
     **/
    private static synchronized void saveXML(Element rootElement, File containerFile)
            throws IOException {
        try {
            String filePath = containerFile.getCanonicalPath();
            String filePathWithoutExtention = filePath.substring(0, filePath.lastIndexOf('.'));
            /**
             * Renaming xxx.xml file to xxx.old file.
             * If xxx.old file exists, the xxx.old file will be deleted for successful renaming.
             */
            File oldFile = new File(filePathWithoutExtention + ".old");
            if (oldFile.exists())
                oldFile.delete();
            else
                containerFile.renameTo(oldFile);

            // creating new xxx.xml file
            PrintStream attrStream = new PrintStream(new FileOutputStream(filePath));
            JSpaceUtilities.domWriter(rootElement, attrStream, "");
            attrStream.close();
        } catch (FileNotFoundException ex) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, "FileNotFoundException: " + ex.toString(), ex);
            }
        }
    }

    private static void setTags(boolean useSchema, String[] tagName, String[] tagValue,
                                String parentTag, String containerName) throws Exception {
        File containerConfigFile = (useSchema ?
                new File(ContainerConfigFactory.getContainerSchemaPathName(containerName)) :
                new File(getContainerConfigPathName(containerName)));

        if (useSchema && !containerConfigFile.exists()) {
            //create container schema file in desired directory if it did not exist there before
            ContainerConfigFactory.createContainerSchemaFile(containerName,
                    containerConfigFile.getPath());
        }

        Document rootDocument = getRootDocument(containerConfigFile);
        Element containerElem = (Element) rootDocument.getElementsByTagName(parentTag).item(0);
        Element elem = null;

        for (int i = 0; i < tagName.length; i++) {
            // update already exists
            if (containerElem.getElementsByTagName(tagName[i]).getLength() > 0) {
                elem = (Element) containerElem.getElementsByTagName(tagName[i]).item(0);
                elem.getChildNodes().item(0).setNodeValue(tagValue[i]);
            }
            // create new tag
            else {
                //elem = rootDocument.createElement(tagName[i]);
                containerElem.appendChild(createTextNode(rootDocument, tagName[i], tagValue[i]));
            }
        }

        saveXML(rootDocument.getDocumentElement(), containerConfigFile);
    }
}
