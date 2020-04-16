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

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

@com.gigaspaces.api.InternalApi
public class ConfigurationParser {
    private DocumentBuilderFactory factory = null;
    private DocumentBuilder builder = null;
    static Logger logger = LoggerFactory.getLogger("com.gigaspaces.start");

    /**
     * Create a new ConfigurationParser, without document validation.
     */
    public ConfigurationParser() throws ParserConfigurationException {
        this(false, Thread.currentThread().getContextClassLoader());
    }

    /**
     * Create a new ConfigurationParser, without document validation.
     *
     * @param loader The parent ClassLoader to use for delegation
     */
    public ConfigurationParser(ClassLoader loader)
            throws ParserConfigurationException {
        this(false, loader);
    }

    /**
     * Create a new ConfigurationParser
     *
     * @param verify If true specifies that the parser produced by this code will validate documents
     *               as they are parsed.
     * @param loader The parent ClassLoader to use for delegation
     */
    public ConfigurationParser(boolean verify, ClassLoader loader)
            throws ParserConfigurationException {
        factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(verify);
        factory.setNamespaceAware(true);
        factory.setExpandEntityReferences(true);
        factory.setCoalescing(true);
        builder = factory.newDocumentBuilder();
        if (verify)
            builder.setErrorHandler(new XMLErrorHandler());
        if (loader == null)
            throw new NullPointerException("loader is null");
    }

    /**
     * Parse an XML Configuration from a File
     *
     * @param xmlFile A File object for an XML file
     * @return An array of String objects parsed from an XML file.
     */
    public String[] parseConfiguration(File xmlFile)
            throws SAXException, IOException {
        Document document = builder.parse(xmlFile);
        return (parseConfiguration(document));
    }

    /**
     * Parse an XML Configuration from a File an URL location.
     *
     * @param xmlURL URL location of the XML file
     * @return An array of String objects parsed from an XML document loaded from the URL.
     */
    public String[] parseConfiguration(URL xmlURL)
            throws IOException, SAXException {
        InputStream is = xmlURL.openStream();
        Document document = null;
        try {
            document = builder.parse(is);
        } finally {
            is.close();
        }
        return (parseConfiguration(document));
    }

    /**
     * Parse an XML Configuration from a File a String location.
     *
     * @param xmlLocation String location of the XML file. The parameter passed in can either point
     *                    to an URL (prefixed by http) or a file found in the classpath.
     * @return An array of String objects parsed from an XML document loaded from the location. This
     * method should be used if the XML document is based on the <code>rio_opstring.dtd</code>
     * document type definition
     */
    public String[] parseConfiguration(String xmlLocation)
            throws IOException, SAXException {
        URL fileURL = null;

        if (xmlLocation.startsWith("http") ||
                xmlLocation.startsWith("file:")) {
            fileURL = new URL(xmlLocation);
        } else {
            fileURL = new File(xmlLocation).toURI().toURL();
        }

        if (fileURL == null)
            throw new FileNotFoundException("XML Location [" + xmlLocation
                    + "] not found");
        return (parseConfiguration(fileURL));
    }

    /**
     * Parse an XML Configuration from a Document
     *
     * @param document The DOM org.w3c.dom.Document object from an XML document
     * @return An array of String objects parsed from an XML document
     */
    public String[] parseConfiguration(Document document) {
        NodeList nList = document.getElementsByTagName("Configuration");
        if (nList.getLength() == 0) {
            nList = document.getElementsByTagName("configuration");
        }
        if (nList.getLength() == 0) {
            nList = document.getElementsByTagName("Overrides");
        }
        if (nList.getLength() == 0) {
            nList = document.getElementsByTagName("overrides");
        }
        List configList = new ArrayList();
        for (int i = 0; i < nList.getLength(); i++) {
            Element element = (Element) nList.item(i);
            configList.addAll(parseConfiguration(element));
        }
        return ((String[]) configList.toArray(new String[configList.size()]));
    }

    /**
     * Parse a Configuration entry
     *
     * @return A List of configuration assignments
     */
    public static List parseConfiguration(Element element) {
        String componentName = null;
        List configList = new ArrayList();
        NodeList nList = element.getChildNodes();
        for (int i = 0; i < nList.getLength(); i++) {
            Node node = nList.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element el = (Element) node;
                if (el.getTagName().equalsIgnoreCase("component")) {
                    NamedNodeMap nm = nList.item(i).getAttributes();
                    Node node1 = nm.getNamedItem("Name");
                    if (node1 == null)
                        node1 = nm.getNamedItem("name");
                    componentName = node1.getNodeValue();
                    if (!componentName.endsWith("."))
                        componentName = componentName + ".";
                    componentName = componentName.replace(' ', '_');
                    Map parms = parseParameters(el);
                    for (Iterator it = parms.entrySet().iterator(); it.hasNext(); ) {
                        Map.Entry me = (Map.Entry) it.next();
                        String name = (String) me.getKey();
                        String value = (String) me.getValue();
                        configList.add(componentName + name + "=" + value);
                    }
                }
            }
        }
        return (configList);
    }

    /**
     * Parse the <Parameters> element
     *
     * @param element The <Parameters> element
     * @return A Properties object containing name/value pairs
     */
    static Properties parseParameters(Element element) {
        Properties props = new Properties();
        String name = null;
        String value = null;
        NodeList nList = element.getElementsByTagName("Parameter");
        if (nList.getLength() == 0)
            nList = element.getElementsByTagName("parameter");
        for (int i = 0; i < nList.getLength(); i++) {
            Element parameterElement = (Element) nList.item(i);
            NamedNodeMap nm = parameterElement.getAttributes();
            Node node = nm.getNamedItem("Name");
            if (node == null)
                node = nm.getNamedItem("name");
            name = node.getNodeValue();
            String elementValue = getTextValue(parameterElement);
            node = nm.getNamedItem("Value");
            if (node == null)
                node = nm.getNamedItem("value");
            if (elementValue.length() > 0 && node != null)
                throw new IllegalArgumentException("Declare either a Value " +
                        "attribute or a <Parameter> " +
                        "value, not both");
            if (node == null) {
                if (elementValue.length() > 0) {
                    value = elementValue;
                } else {
                    throw new IllegalArgumentException("You must declare a Value " +
                            "attribute or a " +
                            "<Parameter> value");
                }
            } else {
                value = node.getNodeValue();
            }
            //System.out.println("### name : "+name+", value : "+value);
            props.put(name, value);
        }

        if (logger.isTraceEnabled()) {
            String tagName = element.getTagName();
            if (element.getParentNode() != null &&
                    element.getParentNode() instanceof Element)
                tagName = ((Element) element.getParentNode()).getTagName() + "." + tagName;
            logger.trace("Element [" + tagName + "], " +
                    "Parameters: " + props.toString());
        }
        return (props);
    }

    /**
     * Get the text value for a node
     *
     * @param node The Node to get the text value for
     * @return The text value for the Node, or a zero-length String if the Node is not recognized
     */
    public static String getTextValue(Node node) {
        NodeList eList = node.getChildNodes();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < eList.getLength(); i++) {
            Node n = eList.item(i);
            if (n.getNodeType() == Node.ENTITY_REFERENCE_NODE) {
                sb.append(getTextValue(n));
            } else if (n.getNodeType() == Node.TEXT_NODE) {
                sb.append(n.getNodeValue());
            }
        }
        return (sb.toString().trim());
    }

    /**
     * An ErrorHandler for parsing
     */
    static class XMLErrorHandler implements ErrorHandler {

        public void warning(final SAXParseException err) throws SAXException {
            System.out.println("+++ Warning"
                    + ", line " + err.getLineNumber()
                    + ", uri " + err.getSystemId());
            System.out.println("   " + err.getMessage());
        }

        public void error(final SAXParseException err) throws SAXException {
            System.out.println("+++ Error"
                    + ", line " + err.getLineNumber()
                    + ", uri " + err.getSystemId());
            System.out.println("   " + err.getMessage());
            throw err;
        }

        public void fatalError(final SAXParseException err) throws SAXException {
            System.out.println("+++ Fatal"
                    + ", line " + err.getLineNumber()
                    + ", uri " + err.getSystemId());
            System.out.println("   " + err.getMessage());
            throw err;
        }

    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("A proper invocation includes passing at least one " +
                    "argument, of which should be an xml file");
            System.exit(1);
        }

        try {
            ConfigurationParser cp = new ConfigurationParser();
            String[] result = cp.parseConfiguration(args[0]);
            for (int i = 0; i < result.length; i++)
                System.out.println(result[i]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
