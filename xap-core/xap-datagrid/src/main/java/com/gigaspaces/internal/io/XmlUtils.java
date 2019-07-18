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

package com.gigaspaces.internal.io;

import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.SystemProperties;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * Provides XML services.
 *
 * @author Niv Ingberg
 * @since 7.0.1
 */
public abstract class XmlUtils {
    // Constants used for JAXP 1.2
    private static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
    private static final String JAXP_SCHEMA_LOCATION = "http://apache.org/xml/properties/schema/external-noNamespaceSchemaLocation";
    private static final String W3C_XML_SCHEMA = "http://www.w3.org/2001/XMLSchema";
    private static final String VALID_SCHEMA_FILE = "clusterXML-Schema.xsd";

    private static DocumentBuilderFactory _docBuilderFactory;
    private static DocumentBuilderFactory _docBuilderFactoryValidated;

    private static final ErrorHandler _defaultErrorHandler = new DefaultErrorHandler();

    /**
     * if -Dcom.gs.XMLEnvCheck=true we flush info about the JAXP environment and print it to a file
     * called GS_JAXP_EnvironmentCheck.xml in current directory.
     */
    private static boolean _printJAXPDebug = Boolean.getBoolean(SystemProperties.JAXP_ENV_DEBUG_REPORT);

    public static DocumentBuilder getDocumentBuilder()
            throws ParserConfigurationException {
        return getDocumentBuilder(false);
    }

    public static String nodeToString(Node node) throws TransformerException {
        StringWriter sw = new StringWriter();
        Transformer t = TransformerFactory.newInstance().newTransformer();
        t.transform(new DOMSource(node), new StreamResult(sw));
        return sw.toString();
    }

    public static DocumentBuilder getDocumentBuilder(boolean doValidation)
            throws ParserConfigurationException {
        DocumentBuilderFactory factory = getDocumentBuilderFactory(doValidation);
        DocumentBuilder documentBuilder = factory.newDocumentBuilder();
        documentBuilder.setErrorHandler(_defaultErrorHandler);

        return documentBuilder;
    }

    private static synchronized DocumentBuilderFactory getDocumentBuilderFactory(boolean doValidation) {
        DocumentBuilderFactory factory;

        if (!doValidation) {
            if (_docBuilderFactory == null)
                _docBuilderFactory = createDocumentBuilderFactory(doValidation);

            factory = _docBuilderFactory;
        } else {
            if (_docBuilderFactoryValidated == null)
                _docBuilderFactoryValidated = createDocumentBuilderFactory(doValidation);

            factory = _docBuilderFactoryValidated;
        }

        return factory;
    }

    private static DocumentBuilderFactory createDocumentBuilderFactory(boolean doValidation) {
        DocumentBuilderFactory factory;

        try {
            factory = DocumentBuilderFactory.newInstance();
        } catch (FactoryConfigurationError e) {
            System.err.println("Warning: Failed to use default DocumentBuilderFactory implementation. " +
                    "\nWill attempt to use org.apache.xerces.jaxp.DocumentBuilderFactoryImpl and org.apache.xerces.jaxp.SAXParserFactoryImpl: " + e.getMessage());
            //xerces parser
            System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "org.apache.xerces.jaxp.DocumentBuilderFactoryImpl");
            System.setProperty("javax.xml.parsers.SAXParserFactory", "org.apache.xerces.jaxp.SAXParserFactoryImpl");
            //try another time using Apache implementation
            factory = DocumentBuilderFactory.newInstance();
        }

        if (doValidation) {
            factory.setValidating(true);
            factory.setAttribute(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
            factory.setAttribute(JAXP_SCHEMA_LOCATION, ResourceLoader.getResourceURL(VALID_SCHEMA_FILE).toString());
        }

        factory.setNamespaceAware(true);
        factory.setIgnoringElementContentWhitespace(true);
        factory.setIgnoringComments(true);

        return factory;
    }

    /**
     * Error handler to report errors and warnings.
     **/
    private static class DefaultErrorHandler implements ErrorHandler {
        public void warning(SAXParseException e) throws SAXException {
            System.err.println("Parsing warning: " + formatException(e));
        }

        public void error(SAXParseException e) throws SAXException {
            throw new SAXException("Parsing error: " + formatException(e));
        }

        public void fatalError(SAXParseException e) throws SAXException {
            throw new SAXException("Parsing fatal error: " + formatException(e));
        }

        // Returns a string describing parse exception details.
        private String formatException(SAXParseException spe) {
            String systemId = spe.getSystemId();
            if (systemId == null)
                systemId = "null";

            Exception ex = spe.getException();
            String exceptionStr = (ex != null ? ex.toString() : "");

            String info = "URI=" + systemId +
                    "  Line= " + spe.getLineNumber() +
                    "  Column= " + spe.getColumnNumber() +
                    "  message: " + spe.getMessage() +
                    "  exception: " + exceptionStr;

            return info;
        }
    }

    public static String getAttribute(Node node, String attributeName) {
        return getAttribute(node, attributeName, null);
    }

    public static String getAttribute(Node node, String attributeName, String defaultValue) {
        Node attribute = node.getAttributes().getNamedItem(attributeName);
        return attribute != null ? attribute.getTextContent().trim() : defaultValue;
    }

    public static Boolean getAttributeBoolean(Node node, String attributeName) {
        String value = getAttribute(node, attributeName, null);
        return (value == null ? null : Boolean.valueOf(value));
    }

    public static boolean getAttributeBoolean(Node node, String attributeName, boolean defaultValue) {
        String value = getAttribute(node, attributeName, null);
        return (value == null ? defaultValue : Boolean.parseBoolean(value));
    }

    public static Integer getAttributeInteger(Node node, String attributeName) {
        String value = getAttribute(node, attributeName, null);
        return (value == null ? null : Integer.valueOf(value));
    }

    public static int getAttributeInteger(Node node, String attributeName, int defaultValue) {
        String value = getAttribute(node, attributeName, null);
        return (value == null ? defaultValue : Integer.parseInt(value));
    }

    public static <T extends Enum<T>> T getAttributeEnum(Node node, String attributeName, Class<T> enumClass) {
        return getAttributeEnum(node, attributeName, enumClass, null, false);
    }

    public static <T extends Enum<T>> T getAttributeEnum(Node node, String attributeName, Class<T> enumClass, T defaultValue) {
        return getAttributeEnum(node, attributeName, enumClass, defaultValue, false);
    }

    public static <T extends Enum<T>> T getAttributeEnum(Node node, String attributeName, Class<T> enumClass, T defaultValue, boolean throwOnError) {
        String value = getAttribute(node, attributeName, null);
        if (value == null || value == "")
            return defaultValue;

        try {
            return Enum.valueOf(enumClass, value.toUpperCase());
        } catch (IllegalArgumentException e) {
            if (throwOnError)
                throw e;

            // TODO: Log warning.
            return defaultValue;
        }
    }

    public static void forEachChild(Node node, String name, Consumer<Node> processor) {
        NodeList nodeList = node.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node child = nodeList.item(i);
            if (child.getNodeName().equals(name)) {
                processor.accept(child);
            }
        }
    }

    public static <T> List<T> mapChildren(Node node, String name, Function<Node, T> processor) {
        List<T> result = new ArrayList<>();
        NodeList nodeList = node.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node child = nodeList.item(i);
            if (child.getNodeName().equals(name)) {
                result.add(processor.apply(child));
            }
        }
        return result;
    }
}
