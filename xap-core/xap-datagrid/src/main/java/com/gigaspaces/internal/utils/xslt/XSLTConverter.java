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

package com.gigaspaces.internal.utils.xslt;

//import java.io.BufferedWriter;

import com.gigaspaces.internal.io.XmlUtils;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.SystemProperties;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

/**
 * Utility Class that provides methods for DOM <--> XML <--> XSL conversions/transformations. e.g. :
 * 1. DOM InputStream and XSL --> transform to a DOM Node (or an XML file) 2. DOM Document    and
 * XSL --> transform to a DOM Node (or an XML file) 3. DOM InputStream and XSL --> transform to a
 * XSL results.
 *
 * The underlying parsing processor is any standat XSLT/SAX parser, such as the Xalan or the
 * defaults coming with the JDK 5.0.
 *
 * This class is used for the cluster schema transformation, from XML/DOM input together with
 * cluster schema XSL file, results in a DOM xml Node, used in the ClusterXML.java.
 *
 * @author Gershon Diner
 * @version 5.0
 */
@com.gigaspaces.api.InternalApi
public class XSLTConverter {
    static private TransformerFactory tFactory;

    static private SchemaFactory schemaFactory;
    static private Schema schemaXSD;

    //check depends system property if need to validate XML versus XML schema
    static private final boolean validXMLSchema = Boolean.parseBoolean(System.getProperty(
            SystemProperties.CLUSTER_XML_SCHEMA_VALIDATION,
            SystemProperties.CLUSTER_XML_SCHEMA_VALIDATION_DEFAULT));

    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONFIG);

    /**
     * Although its a general porpuse method, it is currently used for the cluster schema
     * transformation. It gets XML/DOM InputStream, a XSL schema InputStream, calling the XSLT
     * parser transformation, it results in a DOM xml Node. (Used in the ClusterXML.java for
     * instance)
     *
     * One may decide to use different parser implementation using the Sys prop:
     * -Djavax.xml.transform.TransformFactory=org.apache.xerces.jaxp.TransformFactoryImpl
     *
     * @return a DOM Node (DOM Document)
     */
    static public Node transformDOM2DOM(InputStream _DOMInputStream, InputStream _XSLInputStream)
            throws TransformerException,
            TransformerConfigurationException,
            FileNotFoundException, IOException, ParserConfigurationException {
        // Use the static TransformerFactory.newInstance() method to instantiate 
        // a TransformerFactory. The javax.xml.transform.TransformerFactory 
        // system property setting determines the actual class to instantiate --
        // org.apache.xalan.transformer.TransformerImpl.
        if (tFactory == null)
            tFactory = TransformerFactory.newInstance();

        // Use the TransformerFactory to instantiate a Transformer that will work with  
        // the stylesheet specified by the _XSLInputStream. This method call also processes the XSL stylesheet
        // into a compiled Templates object.
        Transformer transformer = tFactory.newTransformer(new StreamSource(_XSLInputStream));
        //serializer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

        // Use the Transformer to apply the associated Templates object to an XML document
        // (foo.xml) and write the output to a DOM object.
        Document convertedDoc = XmlUtils.getDocumentBuilder().newDocument();
        DOMResult sr = new DOMResult(convertedDoc);
        StreamSource ss = new StreamSource(_DOMInputStream);
        transformer.transform(ss, sr);

        //    Obtaining a org.w3c.dom.Document from XML
        /*DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document transDoc = builder.parse( transURL.openStream() );*/
        Node retDOMnode = sr.getNode();
        //removes all white spaces between XML tags
        //JSpaceUtilities.normalize( retDOMnode );
        return retDOMnode;
    }

    static private void initClusterXSDSchema() throws SAXException, IllegalArgumentException {
        if (schemaXSD == null) {
            // build an XSD-aware SchemaFactory
            schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            // hook up org.xml.sax.ErrorHandler implementation.
            schemaFactory.setErrorHandler(new DefaultErrorHandler());
            // get the custom xsd schema describing the required format for cluster XML files.
            schemaXSD = schemaFactory.newSchema(ResourceLoader.getResourceURL("clusterXML-Schema.xsd"));
        }
    }

    /**
     * If -Dcom.gs.xmlschema.validation=true (which is the default) it will parse the final xml
     * configuration document (XML DOM tree) against the stricter XSD schema (the
     * clusterXML-Schema.xsd schema). Throws exception if it fails to validate or can't create the
     * validator.
     */
    static public void validateClusterDocument(Node validatedXmlDocument)
            throws SAXException, IOException {
        if (validXMLSchema) {
            Validator validator = null;
            try {
                initClusterXSDSchema();
            } catch (SAXException saxE) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            "Failed to create XML xsd validator. Will not validate the cluster configuration due to:  "
                                    + saxE.toString(),
                            saxE);
                }
            } catch (IllegalArgumentException iae) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            "Failed to create XML xsd validator. Will not validate the cluster configuration due to:  "
                                    + iae.toString(),
                            iae);
                }
            }

            if (schemaXSD != null) {
                // Create a Validator capable of validating XML files according to
                // my custom schema.
                validator = schemaXSD.newValidator();
                validator.setErrorHandler(new DefaultErrorHandler());


                // A workaround of a xerces bug once having xmlns attr (which we have
                //with pre GS 6.5 cluster schemas).
                //The cluster schema xsl file of 6.0.3 needs to remove the following xsl stylesheet header:
                //xmlns="http://www.w3.org/2005/02/xpath-functions"
                //If the user will still have this header attribute from previous versions and will not remove
                //it the system will still execute and run properly but the cluster configuration XSL will not be invalidated.
                Attr xpathAttr = null;
                boolean exceptionThrown = false;
                try {
                    Node attrNode = validatedXmlDocument.getFirstChild();
                    if (attrNode != null && attrNode instanceof Element) {
                        Attr attr = (Attr) attrNode.getAttributes().getNamedItem("xmlns");
                        if (attr != null) {
                            xpathAttr = ((Element) attrNode).removeAttributeNode(attr);
                            if (xpathAttr != null && _logger.isDebugEnabled()) {
                                _logger.debug(
                                        "In order the cluster schema xsl validation succeeds you need to remove the following xsl " +
                                                " header attribute. \n" +
                                                xpathAttr +
                                                "\nCurrently we bypassed the validation and continued as expected.");
                            }
                        }
                    }
                } catch (Exception e) {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(
                                "In order the cluster schema xsl validation succeeds you need to remove the following xsl "
                                        + " header attribute. \n"
                                        + xpathAttr
                                        + "\nException thrown before validating: " + e.getMessage());
                    }
                    exceptionThrown = true;
                }

                if (xpathAttr == null && !exceptionThrown) {
                    // parse the XML DOM tree againts the stricter XSD schema
                    if (validator != null && validXMLSchema)
                        validator.validate(new DOMSource(validatedXmlDocument));
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(
                                "Validated successfully the cluster configuration using the XML xsd validator: "
                                        + validator.toString());
                    }
                }
            }
        }
    }


    /**
     * Although its a general porpuse method, it is currently used for the cluster schema
     * transformation. It gets XML/DOM Document, an XSL schema InputStream, calling the XSLT parser
     * transformation, it results in a DOM xml Node. (Used in the ClusterXML.java for instance) One
     * may decide to use different parser implementation using the Sys prop:
     * -Djavax.xml.transform.TransformFactory=org.apache.xerces.jaxp.TransformFactoryImpl
     *
     * @param _DOMDoc an XML DOM Document
     * @return a DOM Node (DOM Document)
     * @throws SAXException in case an xsd validation fails for some reason
     */
    static public Node transformDOM2DOM(Document _DOMDoc, InputStream _XSLInputStream)
            throws TransformerException,
            TransformerConfigurationException,
            FileNotFoundException, IOException, ParserConfigurationException, SAXException {
        // Use the Transformer to apply the associated Templates object to an XML document
        // (foo.xml) and write the output to a DOM object.
        Document convertedDoc = XmlUtils.getDocumentBuilder().newDocument();
        /**
         * FOR DEBUG: Support for xalan tracing - by default, its disabled
         */
        /*
        //      --------------- FOR DEBUG:
        //add xalan trace listener 
//      Set up a PrintTraceListener object to print to a file.
        java.io.FileWriter fw = new java.io.FileWriter("XSLT_transform_events.log");  
        java.io.PrintWriter pw = new java.io.PrintWriter(fw, true);
        PrintTraceListener ptl = new PrintTraceListener(pw);
        
//      Print information as each node is 'executed' in the stylesheet.
        ptl.m_traceElements = true;
        // Print information after each result-tree generation event.
        ptl.m_traceGeneration = true;
        // Print information after each selection event.
        ptl.m_traceSelection = true;
        // Print information whenever a template is invoked.
        ptl.m_traceTemplates = true;
        // Print information whenever an extension call is made.
        ptl.m_traceExtension = true;
        //--------------- FOR DEBUG:
        */

        //Use the static TransformerFactory.newInstance() method to instantiate 
        // a TransformerFactory. The javax.xml.transform.TransformerFactory 
        // system property setting determines the actual class to instantiate --
        // org.apache.xalan.transformer.TransformerImpl.

        if (tFactory == null) {
            tFactory = TransformerFactory.newInstance();
        }

        if (!tFactory.getFeature(DOMSource.FEATURE) || !tFactory.getFeature(StreamResult.FEATURE)) {
            throw new TransformerConfigurationException("The currently used XSL TransformerFactory does not support required features: "
                    + System.getProperty("javax.xml.transform.TransformerFactory" + " Canceled the cluster XSL configuration transformation."));
            //throw exception since its not supported by this implementation
        }

        // Use the TransformerFactory to instantiate a Transformer that will work with  
        // the stylesheet specified by the _XSLInputStream. This method call also processes the XSL stylesheet
        // into a compiled Templates object.
        Transformer transformer = tFactory.newTransformer(new StreamSource(_XSLInputStream));
        
        /*
        //--------------- FOR DEBUG:
        transformer.setOutputProperty(javax.xml.transform.OutputKeys.METHOD,"text");
        System.out.println("Output method is: " + transformer.getOutputProperty(javax.xml.transform.OutputKeys.METHOD));
        //--------------- FOR DEBUG:
        */

        //JSpaceUtilities.normalize(convertedDoc);
        //JSpaceUtilities.normalize(_DOMDoc);

        DOMResult sr = new DOMResult(convertedDoc);
        DOMSource domSource = new DOMSource(_DOMDoc);

//      Instantiate an Xalan XML serializer and use it to serialize the output DOM to System.out
        //--------------- FOR DEBUG:
        // using a default output format.
        /*org.apache.xml.serializer.Serializer serializer = org.apache.xml.serializer.SerializerFactory.getSerializer
                             (org.apache.xml.serializer.OutputPropertiesFactory.getDefaultMethodProperties("xml"));
        serializer.setOutputStream(System.out);
        serializer.asDOMSerializer().serialize(_DOMDoc.getFirstChild());
        //--------------- FOR DEBUG:
        */

        //--------------- FOR DEBUG:        
        //Cast the Transformer object to TransformerImpl.
        /*if (transformer instanceof TransformerImpl)
        {
           TransformerImpl transformerImpl = (TransformerImpl)transformer;
           TraceManager trMgr = transformerImpl.getTraceManager();
           try
            {
              // Register the TraceListener with a TraceManager associated 
              // with the TransformerImpl.
              trMgr.addTraceListener(ptl);
            }catch (TooManyListenersException e)
            {
               // TODO Auto-generated catch block
               e.printStackTrace();
            }           
           // send result to output
           //transformer.transform(domSource, new StreamResult(System.out));
           TransformerImpl.S_DEBUG = true;
           
           // Perform the transformation --printing information to
           // the events log during the process.
           transformerImpl.transform(domSource, sr);
           
           //Close the PrintWriter and FileWriter of the tracer
           //pw.close();
           //fw.close();
        }
        //--------------- FOR DEBUG:
         */
        
        /*//      Instantiate an Xalan XML serializer and use it to serialize the output DOM to System.out
        // using a default output format.
        org.apache.xml.serializer.Serializer serializer = org.apache.xml.serializer.SerializerFactory.getSerializer
                             (org.apache.xml.serializer.OutputPropertiesFactory.getDefaultMethodProperties("xml"));
        serializer.setOutputStream(System.out);
        serializer.asDOMSerializer().serialize(sr.getNode());*/

        //Obtaining a org.w3c.dom.Document from XML
        /*
        //--------------- FOR DEBUG:
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document transDoc = builder.parse( transURL.openStream() );
        //--------------- FOR DEBUG:
        */
        transformer.transform(domSource, sr);
        Node retDOMnode = sr.getNode();

        //removes all white spaces between XML tags
        //JSpaceUtilities.normalize( retDOMnode );

        // parse the XML DOM tree againts the stricter XSD schema
        validateClusterDocument(retDOMnode);

        return retDOMnode;
    }

    // Error handler to report errors and warnings
    private static class DefaultErrorHandler
            implements ErrorHandler {
        /**
         * Returns a string describing parse exception details
         */
        private String getParseExceptionInfo(SAXParseException spe) {
            int line = spe.getLineNumber();
            int column = spe.getColumnNumber();
            StringBuilder sb = new StringBuilder("Parsing validation error - ");
            if (line != -1)
                sb.append(" line: " + line);
            if (column != -1)
                sb.append(" column: " + column);
            sb.append(" : " + spe.getMessage());
            return sb.toString();
        }

        // The following methods are standard SAX ErrorHandler methods.
        // See SAX documentation for more info.
        public void warning(SAXParseException spe) throws SAXException {
            System.err.println("Warning: " + getParseExceptionInfo(spe));
        }

        public void error(SAXParseException spe) throws SAXException {
            //PrintWriter pw = new PrintWriter(System.out, true);
            //org.apache.xml.utils.DefaultErrorHandler.printLocation(pw, spe);
            //pw.println( message );
            //String message = "Error: " + getParseExceptionInfo(spe);
            throw new SAXException(getParseExceptionInfo(spe));
        }

        public void fatalError(SAXParseException spe) throws SAXException {
            String message = "Fatal Error: " + getParseExceptionInfo(spe);
            throw new SAXException(message);
        }
    }

    public static void main(String[] args)
            throws TransformerConfigurationException, FileNotFoundException,
            TransformerException, IOException, ParserConfigurationException {
        InputStream members = XSLTConverter.class.getResourceAsStream(args[0]);
        InputStream policy = XSLTConverter.class.getResourceAsStream(args[1]);
        Node retNode = XSLTConverter.transformDOM2DOM(members, policy);
//    Use the static TransformerFactory.newInstance() method to instantiate 
        // a TransformerFactory. The javax.xml.transform.TransformerFactory
        // system property setting determines the actual class to instantiate --
        // org.apache.xalan.transformer.TransformerImpl.
        /*TransformerFactory tFactory = TransformerFactory.newInstance();
        
        // Use the TransformerFactory to instantiate a Transformer that will work with  
        // the stylesheet you specify. This method call also processes the stylesheet
      // into a compiled Templates object.
        Transformer transformer = tFactory.newTransformer(new StreamSource("birds.xsl"));

        // Use the Transformer to apply the associated Templates object to an XML document
        // (foo.xml) and write the output to a file (foo.out).
        transformer.transform(new StreamSource("birds.xml"), new StreamResult(new FileOutputStream("birds.out")));*/
    }
}
