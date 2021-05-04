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

package com.j_spaces.kernel;

import com.gigaspaces.cluster.loadbalance.LoadBalancingPolicy;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.io.XmlUtils;
import com.gigaspaces.internal.jvm.JVMDetails;
import com.gigaspaces.internal.jvm.JVMInfoProvider;
import com.gigaspaces.internal.utils.CollectionUtils;
import com.gigaspaces.internal.utils.xslt.XSLTConverter;
import com.gigaspaces.lrmi.nio.info.NIODetails;
import com.gigaspaces.lrmi.nio.info.NIOInfoProvider;
import com.gigaspaces.management.entry.JMXConnection;
import com.j_spaces.core.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.admin.SpaceRuntimeInfo;
import com.j_spaces.core.client.*;
import com.j_spaces.core.cluster.*;
import com.j_spaces.core.exception.ClusterConfigurationException;
import com.j_spaces.core.exception.internal.EngineInternalSpaceException;
import com.j_spaces.core.exception.internal.LeaseInternalSpaceException;
import com.j_spaces.core.exception.internal.ProxyInternalSpaceException;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;
import com.j_spaces.kernel.log.JProperties;
import com.j_spaces.lookup.entry.ClusterName;
import net.jini.admin.JoinAdmin;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.transaction.server.TransactionConstants;
import net.jini.discovery.LookupGroups;
import net.jini.id.ReferentUuid;
import net.jini.id.Uuid;
import net.jini.lookup.entry.Name;
import net.jini.space.InternalSpaceException;
import org.jini.rio.boot.BootUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.text.NumberFormat;
import java.util.*;

import static com.j_spaces.core.Constants.LookupManager.*;

/**
 * JSpaceUtility class provides useful GigaSpaces utility functions.
 *
 * @author Igor Goldenbeg
 * @version 1.0
 */
@com.gigaspaces.api.InternalApi
public class JSpaceUtilities {
    public static final String BLANK_VALUE = "-";
    public static final String YES_VAL = "Yes";
    public static final String NO_VAL = "No";
    public static final String SECURED_HIDDEN_VALUE = "****";

    public static final String NOT_AVAILABLE_STR = "n/a";

    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONFIG);

    /**
     * this tag will be appear in all XML files that container will be created.
     */
    private static final String XML_VERSION_TAG = "<?xml version=\"1.0\"?>";

    /**
     * An empty string constant.
     */
    private static final String EMPTY = "";

    public final static String LINE_SEPARATOR = System.getProperty("line.separator");

    private static Map<ServiceID, Boolean> jmxRemotePortDefiedMap = new HashMap<>();

    /**
     * Order all XML tags of DOM tree and writes XML DOM tree to OutputStream. For example DOM tree
     * looks like this: <a><b>value</b></a>
     *
     * After <code>domWriter</code> function DOM tree will look like this: <a> <b>value</b> </a>
     *
     * @param node   XML DOM tree.
     * @param ps     OutputStream it's can be FileOutputStream, System.out
     * @param prefix Number of space between tags
     * @return <code>true</code> if the node is a value node, <code>false</code> otherwise
     * <code>true</code>
     * @see com.j_spaces.kernel.JSpaceUtilities#normalize(Node node)
     */
    static public boolean domWriter(Node node, PrintStream ps, String prefix) {
        // set default XML version
        ps.print(XML_VERSION_TAG);

        return _domWriter(node, ps, prefix);
    }

    /**
     * Recursive normalize and build XML Dom tree.
     *
     * @see com.j_spaces.kernel.JSpaceUtilities#domWriter(Node node, PrintStream ps, String prefix)
     **/
    static private boolean _domWriter(Node node, PrintStream ps, String prefix) {
        // text nodes are special case
        if (node.getNodeType() == Node.TEXT_NODE) {
            NodeList nl = node.getChildNodes();
            for (int i = 0; i < nl.getLength(); i++) {
                _domWriter(nl.item(i), ps, prefix);
            }

            String nodeValue = node.getNodeValue().trim();
            if (nodeValue != null && nodeValue.length() > 0) {
                ps.print(nodeValue);
                return true;
            }
            return false;
        }

        // comment nodes are special case
        if (node.getNodeType() == Node.COMMENT_NODE) {
            ps.println();
            ps.print(prefix + "<!--" + node.getNodeValue().trim() + "-->");
            return false;
        }

        NodeList nl = node.getChildNodes();
        int numOfChildren = nl.getLength();
        String tagName = getNodeSignature(node);
        ps.println();
        ps.print(prefix + "<" + tagName + ">");
        boolean isMemberUrlTag = tagName.equals(ClusterXML.MEMBER_URL_TAG);


        boolean valueNode = false;
        for (int i = 0; i < numOfChildren; i++) {
            Node childNode = nl.item(i);
            //if tag name is member url so check its value for &
            if (isMemberUrlTag) {
                String memberUrl = childNode.getNodeValue();
                memberUrl = handleInvalidChars(memberUrl);
                childNode.setNodeValue(memberUrl);
            }

            boolean returnedValue = _domWriter(childNode, ps, prefix + "     ");
            valueNode = valueNode || returnedValue;
        }

        if (!valueNode) {
            ps.println();
            ps.print(prefix + "</" + node.getNodeName() + ">");
        } else {
            ps.print("</" + node.getNodeName() + ">");
        }

        return false;
    }

    /**
     * This method constructs name for container representation. Actually it concatenates host name
     * with container name.
     *
     * @param hostName      host name
     * @param containerName container name
     * @return representation name
     */
    public static String createContainerPresentName(String hostName, String containerName) {
        if (hostName == null)
            hostName = "";
        else
            hostName += ":";

        return hostName + containerName;
    }

    /**
     * Returns Node signature with node name and attributes(if exists).
     *
     * @return Node signature as String. For example: user name="user1"  password="11112"
     * roles="A,R"
     **/
    static private String getNodeSignature(Node node) {
        if (!node.hasAttributes())
            return node.getNodeName();
        else {
            String nodeSignature = node.getNodeName() + " ";
            NamedNodeMap attrMap = node.getAttributes();
            for (int i = 0; i < attrMap.getLength(); i++) {
                Node attrNode = attrMap.item(i);
                nodeSignature += attrNode.getNodeName() + "=\"" + attrNode.getNodeValue() + "\" ";
            }

            return nodeSignature.trim();
        }
    }

    /**
     * Parses the string argument as a boolean. The <code>boolean</code> returned represents the
     * value <code>true</code> if the string argument is not <code>null</code>, is not a
     * sys-property, and is equal, ignoring case, to the string <code>"true"</code>.
     *
     * @param tag the <code>xml-tag</code> containing the boolean
     * @param s   the <code>String</code> containing the boolean representation to be parsed
     * @return the boolean represented by the string argument
     * @throws IllegalArgumentException if the string does not contain a parsable boolean.
     */
    public static boolean parseBooleanTag(String tag, String s) {
        return parseBooleanTag(tag, s, false);

    }

    /**
     * Parses the string argument as a boolean. The <code>boolean</code> returned represents the
     * value <code>true</code> if the string argument is not <code>null</code>, is not a
     * sys-property, and is equal, ignoring case, to the string <code>"true"</code>.
     *
     * @param tag          the <code>xml-tag</code> containing the boolean
     * @param s            the <code>String</code> containing the boolean representation to be
     *                     parsed
     * @param defaultValue the default value if property is not defined
     * @return the boolean represented by the string argument
     * @throws IllegalArgumentException if the string does not contain a parsable boolean.
     */
    public static boolean parseBooleanTag(String tag, String s, boolean defaultValue) {
        if (s == null || s.startsWith("${"))
            return defaultValue;

        if (s.equalsIgnoreCase(Boolean.FALSE.toString()))
            return false;

        if (s.equalsIgnoreCase(Boolean.TRUE.toString()))
            return true;

        throw new IllegalArgumentException("Illegal boolean input string: \""
                + s + "\" for tag: <" + tag + ">");
    }

    /**
     * Normalize DOM tree. Remove all white spaces between tags. for example: <a> <b>value</b> </a>
     * Normalize to <a><b>value</b></a>
     *
     * @param node Dom tree.
     * @see com.j_spaces.kernel.JSpaceUtilities#domWriter(Node node, PrintStream ps, String prefix)
     */
    static public void normalize(Node node) {
        for (int i = 0; i < node.getChildNodes().getLength(); i++) {
            Node childNode = node.getChildNodes().item(i);
            if (childNode.getNodeType() == Node.TEXT_NODE && childNode.getNodeValue().trim().length() == 0) {
                node.removeChild(childNode);
                i--;
            } else
                normalize(childNode);
        }
    }

    /**
     * Converts recursively entire DOM tree values from System Property to value. For example let's
     * assume that defined the following System Property: System.setProperty("com.gs.home",
     * "d:/GigaSpaces4.0"); <a> <b>${com.gs.home}/igor/logs</b> </a> After invoking this method: <a>
     * <b>d:/GigaSpaces4.0/igor/logs</b> </a> if no System Property defined as part of Node value,
     * the value stay untouchable.
     *
     * @param node Dom tree.
     * @see com.j_spaces.kernel.JSpaceUtilities#domWriter(Node node, PrintStream ps, String prefix)
     * @see com.j_spaces.kernel.JSpaceUtilities#normalize(Node)
     **/
    static public void convertDOMTreeFromSystemProperty(Node node) {
        for (int i = 0; i < node.getChildNodes().getLength(); i++) {
            Node childNode = node.getChildNodes().item(i);
            if (childNode.getNodeType() == Node.TEXT_NODE) {
                String childNodeValue = childNode.getNodeValue().trim();
                if (childNodeValue.length() != 0) {
                    childNodeValue = JProperties.getPropertyFromSystem(childNodeValue, null);
                    childNode.setNodeValue(childNodeValue);
                }
            } else {
                convertDOMTreeFromSystemProperty(childNode);
            }
        }
    }

    /**
     * Create TextNode.
     *
     * @param tagName  The name of tag <tagName>tagValue</tagName>
     * @param tagValue The tag value of tag name --^
     * @return Node the node consists tagValue.
     **/
    static private Node createTextNode(Document _doc, String tagName, String tagValue) {
        Element tag = _doc.createElement(tagName);
        Text tagText = _doc.createTextNode(tagValue);
        tag.appendChild(tagText);

        return tag;
    }

    /**
     * Creates cluster member tags using the total members and number of backup members if exists.
     *
     * @param _rootDoc         The root document.
     * @param _rootMembersElem The <cluster-members> element
     **/
    static private Node createClusterMembers(Document _rootDoc, Element _rootMembersElem
            , int _totalMembers, int _backupMembers, String _clusterName, String _groups) {
        Element memberTag = null;
         /* By default we use jini: with multicast protocol as prefix to each of the cluster
         * members created dynamically. If the -Dcom.gs.cluster.url-protocol-prefix is passed
         * we use this prefix instead.
         * e.g. when defining -Dcom.gs.cluster.url-protocol-prefix=rmi://pc-gershon:10098/ will have
         * this protocol prefix for all the cluster member urls */
        String memberPrefix = System.getProperty(ClusterXML.CLUSTER_MEMBER_URL_PROTOCOL_PREFIX, ClusterXML.CLUSTER_MEMBER_URL_PROTOCOL_PREFIX_DEF);
        /** add primary members */

        for (int i = 1; i <= _totalMembers; i++)//The first member id starts with 1
        {
            memberTag = _rootDoc.createElement(ClusterXML.MEMBER_TAG);
//          * add backup members to each primary member.
//          * total_members={number of primary instances, number of backup instances per primary}
//          * For example if the _totalMembers value is 4 and _backupMembers 2 it means that this cluster contains
//          * up to 4 primary instances EACH containing 2 backup instances..
            if (_backupMembers > 0) {
                memberTag.setAttribute("backup-container", _clusterName + "_container" + i);
                memberTag.setAttribute("number-backups", Integer.toString(_backupMembers));
                if (_groups != null) {
                    memberTag.setAttribute("jini-groups", "?groups=" + _groups);
                }
                memberTag.setAttribute("member-prefix", memberPrefix);
            }

            Element memberNameTag = _rootDoc.createElement(ClusterXML.MEMBER_NAME_TAG);
            Text memberNameText = _rootDoc.createTextNode(_clusterName + "_container" + i + ":" + _clusterName);

            Element memberUrlTag = _rootDoc.createElement(ClusterXML.MEMBER_URL_TAG);

            Text memberUrlText = null;
            if (_groups != null) {
//              if ?groups=public,mygroup passed, we add it to each member url
                memberUrlText = _rootDoc.createTextNode(memberPrefix + _clusterName + "_container" + i + "/" + _clusterName + "?groups=" + _groups);
            } else {
                memberUrlText = _rootDoc.createTextNode(memberPrefix + _clusterName + "_container" + i + "/" + _clusterName);
            }

            memberTag.appendChild(memberNameTag).appendChild(memberNameText);
            memberTag.appendChild(memberUrlTag).appendChild(memberUrlText);

             /*// create params tags
             if ( memInfo[i].paramData != null )
             {
              for( int j = 0; j < memInfo[i].paramData.size(); j++ )
              {
               Element paramTag  = rootDoc.createElement( ClusterXML.PARAM_TAG );
               String paramName  = (String)((Vector)memInfo[i].paramData.get(j)).get(0);
               String paramValue = (String)((Vector)memInfo[i].paramData.get(j)).get(1);
               paramTag.appendChild( createTextNode( ClusterXML.PARAM_NAME_TAG, paramName ));
               paramTag.appendChild( createTextNode( ClusterXML.PARAM_VALUE_TAG, paramValue ));

               // append param to current member
               memberTag.appendChild( paramTag );
              }// for j..
             }// if
   */
            // append new member to root members
            _rootMembersElem.appendChild(memberTag);
        }// for i..

         /*// append to the <cluster-members> element
         if ( memberTag != null )
            _rootMembersElem.appendChild( memberTag );*/

        return _rootMembersElem;
    }// createClusterMembers


    /**
     * Method which gets cluster xml Dom element and cluster xsl schema input stream. It calls the
     * XSLT converter which produces a full cluster configuration Dom document.
     *
     * @return a full cluster configuration Dom document.
     * @throws SAXException in case an xsd validation fails for some reason
     */
    static public Document convertToClusterConfiguration(Document clusterXMLDomElement, InputStream clusterXSLPolicy)
            throws IOException, TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException {
        Document rootDoc = (Document) XSLTConverter.transformDOM2DOM(clusterXMLDomElement, clusterXSLPolicy);
        return rootDoc;
    }

    /***
     * A method which overrides cluster configuration xml using a XPath key/value pair passed
     * through custom properties. It gets an xml XPath key and using an XPathAPI it attempts to find
     * the relevant DOM node. If it was found it sets a new value. Although its a general method,
     * its used in the context of the cluster configuration override.
     *
     * Supporting both the standard XPath using / as well as "our" XPath standard using dots:
     * -------------------------------------------------------------------------------------- If
     * user passed "real" full XPath, starting with / we pass it as is to the XPathAPI That enables
     * also to support advanced XPath queries according to the spec If user passed a dot ("our"
     * XPath like), e.g. cluster-config.groups.group etc. then we change it to be a standard xpath
     * replacing dots with /
     *
     * @param xpath a XPath string such as /cluster-config/groups/group/load-bal-policy/proxy-broadcast-threadpool-min-size
     *              while in the custom properties we pass it using a dot e.g.:
     *              cluster-config.groups.group.load-bal-policy.proxy-broadcast-threadpool-min-size=33
     * @return same xml DOM but after overwriting all elements using the XPaths new values
     * @throws Exception in case org.apache.xpath.XPathAPI is not found or TransformerException in
     *                   case error during transformation process.
     */
    static public Document overrideClusterConfigWithXPath(String xpath,
                                                          String newValue, Document clusterConfigRootDoc) throws Exception {
        Node firstNode = null;
        NodeList nodes = null;
        try {
            nodes = (NodeList) XPathFactory.newInstance().newXPath().evaluate(xpath, clusterConfigRootDoc, XPathConstants.NODESET);
        } catch (Exception e) {
            _logger.error(
                    "Failed to override the cluster config using the xpath expression <"
                            + xpath
                            + "> passed through the custom properties.", e);
        }
        if (nodes != null) {
            for (int i = 0; i < nodes.getLength(); i++) {
                Node node = nodes.item(i);
                if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
                    firstNode = node;
                } else {
                    // usually, in case this is an element node, get the first node which is the actual text
                    firstNode = node.getFirstChild();
                }
                if (firstNode != null) {
                    String oldNodeValue = firstNode.getNodeValue().trim();
                    firstNode.setNodeValue(newValue);
                    if (_logger.isInfoEnabled()) {
                        _logger.info(
                                "Override the cluster config using the xpath expression <"
                                        + xpath
                                        + "> passed through the custom properties.\n\tOld value: <"
                                        + oldNodeValue + "> \t new value: <"
                                        + newValue + ">");
                    }
                }
            }
        }
        if (firstNode == null && _logger.isErrorEnabled()) {
            throw new ClusterConfigurationException("Failed to override the cluster config using the xpath expression <"
                    + xpath
                    + ">, no element matched the given xpath expression");
        }
        return clusterConfigRootDoc;
    }

    /**
     * @return same xml DOM but after overwriting all elements using the XPaths new values
     */
    static public Document overrideClusterConfigWithXPath(
            Properties customProps, Document clusterConfigRootDoc)
            throws Exception {
        if (customProps != null) {
            for (Enumeration keys = customProps.keys(); keys.hasMoreElements(); ) {
                String propKey = (String) keys.nextElement();
                //if user passed "real" standard full XPath, starting with / we pass it as is to the XPathAPI
                //That enables also to support advanced XPath queries according to the spec
                boolean isStandardXPath = propKey.startsWith("/" + ClusterXML.CLUSTER_CONFIG_TAG) ? true : false;
                //if user passed a dot ("our" XPath like), e.g. cluster-config.groups.group etc.
                //then we change it to be a standard xpath replacing dots with /
                if (isStandardXPath || propKey.startsWith(ClusterXML.CLUSTER_CONFIG_TAG)) {
                    String propValue = customProps.getProperty(propKey);
                    if (!isStandardXPath)
                        propKey = "/" + replaceInString(propKey, ".", "/", true);

                    if (!JSpaceUtilities.isEmpty(propValue))
                        propValue = JSpaceUtilities.removeInvertedCommas(propValue);

                    clusterConfigRootDoc = overrideClusterConfigWithXPath(propKey,
                            propValue,
                            clusterConfigRootDoc);
                }
            }
        }
        return clusterConfigRootDoc;

    }

    /**
     * Builds a schema cluster config xml file on the fly, using several inputs. The xml Dom file
     * which is built, is later used as input to the XSLT processor (which is using it, together
     * with cluster xsl schema file, and produces xml config file full structure, used in the
     * ClusterXML.java)
     *
     * @param _totalMembers      The number of space/cache nodes/spaces in this cluster. The format
     *                           of the total_members={number of primary instances, number of backup
     *                           instances per primary} If ?total_members=4,2 the value is 4,2 means
     *                           that this cluster contains up to 4 primary instances each
     *                           containing 2 backup instances.
     * @param _backupMembers     stands for the SpaceURL backups part of the ?total_numbers option,
     *                           total_members={number of primary instances, number of backup
     *                           instances per primary} In this example the value is 4,2 which means
     *                           that this cluster contains up to 4 primary instances each
     *                           containing 2 backup instances..
     * @param _clusterSchemaName The cluster schema XSL file to be used to be build the cluster XML
     *                           Dom.
     * @param _clusterName       Used also as the cache name.
     * @return a DOM Document object of the cluster-config sub tree
     * @see <code>com.j_spaces.tools.xslt.XSLTConverter</code>
     * @see <code>com.j_spaces.core.cluster.ClusterXML</code>
     */
    static public Document buildClusterXMLDom(int _totalMembers, int _backupMembers, String _clusterSchemaName
            , String _clusterName, String _distCacheConfigName, String _jmsConfigName, String _groups)
            throws ParserConfigurationException {
        //Obtaining a org.w3c.dom.Document from XML
        Document rootDoc = XmlUtils.getDocumentBuilder().newDocument();
        Element clusterConfigTag = rootDoc.createElement(ClusterXML.CLUSTER_CONFIG_TAG);

        Node clusterSchemaTag = createTextNode(rootDoc, ClusterXML.CLUSTER_SCHEMA_NAME_TAG, _clusterSchemaName);
        clusterConfigTag.appendChild(clusterSchemaTag);
        Node clusterNameTag = createTextNode(rootDoc, ClusterXML.CLUSTER_NAME_TAG, _clusterName);
        clusterConfigTag.appendChild(clusterNameTag);


        if (!JSpaceUtilities.isEmpty(_distCacheConfigName)) {
            /** Create Distributed Cache Section **/
            Element dcacheConfigElem = rootDoc.createElement(ClusterXML.DCACHE_TAG);
            dcacheConfigElem.appendChild(createTextNode(rootDoc, ClusterXML.DCACHE_CONFIG_NAME_TAG, _distCacheConfigName));
            clusterConfigTag.appendChild(dcacheConfigElem);
        }

        if (!JSpaceUtilities.isEmpty(_jmsConfigName)) {
            /** Create JMS Section **/
            Element jmsConfigElem = rootDoc.createElement(ClusterXML.JMS_TAG);
            jmsConfigElem.appendChild(createTextNode(rootDoc, ClusterXML.JMS_CONFIG_NAME_TAG, _jmsConfigName));
            clusterConfigTag.appendChild(jmsConfigElem);
        }

        Node notifyRecoveryTag = createTextNode(rootDoc, ClusterXML.NOTIFY_RECOVERY_TAG,
                ClusterXML.NOTIFY_RECOVERY_DEFAULT_VALUE);
        clusterConfigTag.appendChild(notifyRecoveryTag);

        /** CREATES CLUSTER MEMBERS TAG **/
        //System.out.println("DEBUG: Before CREATES CLUSTER MEMBERS TAGS");
        Element clusterMembersTag = rootDoc.createElement(ClusterXML.CLUSTER_MEMBERS_TAG);
        clusterConfigTag.appendChild(clusterMembersTag);
        createClusterMembers(rootDoc, clusterMembersTag, _totalMembers, _backupMembers, _clusterName, _groups);

        rootDoc.appendChild(clusterConfigTag);
        return rootDoc;
    }

    /**
     * Setting the XML parser and converter implementations. We use xerces parser and xalan XSLT
     * transformer implementations Make sure the xercesImpl.jar and xalan.jar files are defined in
     * the classpath.
     *
     * We check if this setting needs to be forcibly executed. If sys prop
     * -Dcom.gs.dont-use-default-xml-parser=true it forces setting these implementations. In such
     * case the system will use the fallback implementation of the JDK.
     *
     * 19.12.05 - Gershon: We no longer force by DEFAULT the xerces and xalan implementations and
     * this sys prop setting is kept only for specific scenarios where the currently used
     * implementation does not support the Transformation and parsing needs. In such case you may
     * pass the -Dcom.gs.dont-use-default-xml-parser=true system property to force setting these
     * implementations. In such case the system will use the fallback implementation of the JDK (if
     * no -Xbootclasspath was set either).
     *
     * NOTE that sys prop was initially called com.gs.use-default-xml-parser.
     */
    static public void setXMLImplSystemProps() {
        //NOTE -Dcom.gs.dont-use-default-xml-parser=true we DO FORCE setting

        //NOTE -Dcom.gs.dont-use-default-xml-parser=false or sys prop
        //not exist we DO NOT FORCE ANY setting
        if (Boolean.getBoolean("com.gs.dont-use-default-xml-parser")) {
            //xalan XSLT transformer
            System.setProperty("javax.xml.transform.TransformerFactory",
                    "org.apache.xalan.processor.TransformerFactoryImpl");
        }
    }

    /**
     * Replace the desired string in all <code>content</code> string. <b>This method ignoring Case
     * Sensitive</b>.
     *
     * For example: String content = "Hello World Hello World say Igor 1000 times"; String replStr =
     * replaceInString(content,"Hello", "I Love", true); Output ==> "I Love I Love say Igor 1000
     * times"
     *
     * @param content    The content string where will be executing replacing.
     * @param oldStr     The string which should find in <code>content</code> and replace.
     * @param newStr     The string which will be replaced with <code>old</code> string.
     * @param replaceAll <code>true</code> if replace in all <code>content</code>,
     *                   <code>false</code> the replace will be executed only one time.
     * @return Returns the replaced content or the original content if was nothing replaced.
     **/
    static public String replaceInString(String content, String oldStr, String newStr, boolean replaceAll) {
        int startInx = 0;
        String lowCaseContent = content.toLowerCase();
        StringBuilder originalCotext = new StringBuilder(content);

        String lowSearchStr = oldStr.toLowerCase();

        while ((startInx = lowCaseContent.indexOf(lowSearchStr, startInx)) != -1) {
            int endInx = startInx + oldStr.length();
            content = originalCotext.replace(startInx, endInx, newStr).toString();
            lowCaseContent = originalCotext.toString().toLowerCase();
            startInx = endInx;

            if (!replaceAll)
                return content;
        }

        return content;
    }


    /**
     * This method goes through passed string and replace each occurrence of '&' to '&amp;'
     *
     * @param str strin to be replaced
     * @return string with valid characters
     */
    private static String handleInvalidChars(String str) {
        String result = "";
        final String replaceAmpresand1 = "&";
        final String replaceAmpresand2 = "amp;";
        final String replaceAmpresand = replaceAmpresand1 + replaceAmpresand2;

        final int ampLength = replaceAmpresand2.length();
        StringTokenizer strTokinizer = new StringTokenizer(
                str, String.valueOf(replaceAmpresand1), true);
        while (strTokinizer.hasMoreTokens()) {
            String token = strTokinizer.nextToken();

            if (token.startsWith(replaceAmpresand2)) {
                token = token.substring(ampLength);
            } else if (token.equals(replaceAmpresand1)) {
                token = replaceAmpresand;
            }

            result += token;
        }

        return result;
    }

    /**
     * Converts the stack trace of the specified exception to a string. NOTE: Pass the exception as
     * an argument to the external exception
     */
    public static String getStackTrace(Throwable t) {
        return BootUtil.getStackTrace(t);
    }

    public static String getCallStackTraces( int deep ) {
        final int shift = 2;//allows not to display call to this method and this call itself
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        StringBuilder stringBuilder = new StringBuilder("\n");
        for( int i = 0; i < deep && i + shift < stackTrace.length; i++ ){
            stringBuilder.append( stackTrace[ i + shift ] );
            stringBuilder.append( "\n" );
        }

        return stringBuilder.toString();
    }

    public static String getPropertiesPresentation(Properties props) {
        if (props == null) {
            return "";
        }

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        props.list(pw);
        pw.flush();

        return sw.toString();
    }


    /**
     * This method checks whole hierarchy for the exception cause. If it finds the exception of
     * specific class, method returns exception message if exists such one otherwise null returned.
     *
     * @param sourceException          hierarchy of cause exception belongs to this exception will
     *                                 be checked
     * @param checkCauseExceptionClass Class instance
     * @return if there is exception of specific class in exception hierarchy its message will be
     * returned, otherwise null returned
     */
    public static String getCauseExceptionMessageFromHierarchy(Exception sourceException, Class checkCauseExceptionClass) {
        Throwable causeException = sourceException;
        while ((causeException = causeException.getCause()) != null) {
            if (causeException.getClass().equals(checkCauseExceptionClass)) {
                String message = causeException.getMessage();
                //check if message is not null and not empty
                if (isEmpty(message))
                    message = causeException.toString();

                return message;
            }
        }
        return null;
    }


    /**
     * This method checks whole hierarchy for the exception cause. If it finds the exception of
     * specific class, method returns this exception if exists such one otherwise null returned.
     *
     * @param sourceException          hierarchy of cause exception belongs to this exception will
     *                                 be checked
     * @param checkCauseExceptionClass Class instance
     * @return if there is exception of specific class in exception hierarchy it will be returned,
     * otherwise null returned
     */
    public static Throwable getCauseExceptionFromHierarchy(Exception sourceException, Class checkCauseExceptionClass) {
        Throwable causeException = sourceException;
        while ((causeException = causeException.getCause()) != null) {
            if (causeException.getClass().equals(checkCauseExceptionClass)) {
                return causeException;
            }
        }
        return null;
    }

    public static Throwable getAssignableCauseExceptionFromHierarchy(Exception sourceException, Class checkCauseExceptionClass) {
        Throwable causeException = sourceException;
        while ((causeException = causeException.getCause()) != null) {
            if (checkCauseExceptionClass.isAssignableFrom(causeException.getClass())) {
                return causeException;
            }
        }
        return null;
    }

    public static Throwable getRootCauseException(Throwable sourceException) {
        Throwable causeException = sourceException;
        while (causeException.getCause() != null) {
            causeException = causeException.getCause();
        }
        return causeException;
    }

    public static boolean isSameException(Throwable exception1, Throwable exception2) {
        if (exception1 == exception2)
            return true;

        if (exception1 == null || exception2 == null)
            return false;

        if (!exception1.getClass().equals(exception2.getClass()))
            return false;

        if (!exception1.getMessage().equals(exception2.getMessage()))
            return false;

        return (Arrays.equals(exception1.getStackTrace(), exception2.getStackTrace()));
    }

    /**
     * Check if the given string is empty or null.
     *
     * @param s String to check
     * @return True if string is empty or null
     */
    public static boolean isEmpty(String s) {
        return isEmpty(s,true);
    }

    public static boolean isEmpty(String s, boolean isWithTrim) {
        if (s == null || s.length() == 0)
            return true;

        if (isWithTrim)
            s = s.trim();

        return s.length() == 0;
    }

    public static boolean isObjectEquals(Object obj1, Object obj2) {
        // Check if same reference:
        if (obj1 == obj2)
            return true;
        // Otherwise check if either is null:
        if (obj1 == null || obj2 == null)
            return false;
        // Otherwise, check equals:
        return obj1.equals(obj2);
    }

    public static boolean isStringEquals(String s1, String s2) {
        // Check if same reference:
        if (s1 == s2)
            return true;
        // Otherwise check if either is null:
        if (s1 == null || s2 == null)
            return false;

        if (isEmpty(s1) && isEmpty(s2))
            return true;

        return s1.trim().equals(s2.trim());
    }

    public static boolean isStringEqualsIgnoreCase(String s1, String s2) {
        // Check if same reference:
        if (s1 == s2)
            return true;
        // Otherwise check if either is null:
        if (s1 == null || s2 == null)
            return false;

        if (isEmpty(s1) && isEmpty(s2))
            return true;

        return s1.trim().equalsIgnoreCase(s2.trim());
    }


    public static ServiceID getSpaceServiceID(IJSpace space) {
        return getServiceID((ReferentUuid) space);
    }


    public static ServiceID getServiceID(ReferentUuid service) {
        Uuid uuid = service.getReferentUuid();
        return new ServiceID(uuid.getMostSignificantBits(),
                uuid.getLeastSignificantBits());

    }

    /**
     * Removes Inverted Commas from the string.
     *
     * @return same string without Inverted Commas
     */
    public static String removeInvertedCommas(String s) {
        return removeDelimiter(s, '\"');
    }

    /**
     * Removes Inverted Commas from the string.
     *
     * @param delim the delimiters.
     * @return same string without Inverted Commas
     */
    public static String removeDelimiter(String s, char delim) {
        return removeDelimiter(s, delim, (char) -1, (char) -1);
    }

    /**
     * Removes delimiter from the string ignores parts in the escapes.
     *
     * @param delim the delimiters.
     * @return same string without Delimiters and escape chars
     */
    public static String removeDelimiter(String s, final char delim, final char openEscape, final char closeEscape) {
        StringBuilder t = new StringBuilder(s);
        boolean escaped = false;

        for (int i = 0; i < t.length(); ++i) {
            char c = t.charAt(i);
            if (escaped) {
                if (c == closeEscape) // found close escape
                {
                    escaped = false;
                }
                //else in escape block
            } else if (c == openEscape) // found open escape
            {
                escaped = true;
            } else if (c == delim) {
                t.deleteCharAt(i--); // remove delimiter
            }
            //else leave char
        }

        return t.toString();
    }

    public static String getJiniGroupRepresentation(IRemoteJSpaceAdmin spaceAdmin)
            throws RemoteException {
        String jiniGroupRepresentation = "";
        if (spaceAdmin instanceof JoinAdmin) {
            JoinAdmin joinAdmin = (JoinAdmin) spaceAdmin;
            String[] lookupGroups = joinAdmin.getLookupGroups();
            for (String groupName : lookupGroups) {
                if (jiniGroupRepresentation.length() == 0) {
                    jiniGroupRepresentation = groupName;
                } else {
                    jiniGroupRepresentation += "; " + groupName;
                }
            }
        }

        return jiniGroupRepresentation;
    }


    public static Object getObjectFromSpaceByUid(IJSpace spaceProxy, String uid)
            throws Exception {
        ExternalEntry entry =
                (ExternalEntry) ((ISpaceProxy) spaceProxy).readByUid(uid, null, ReadModifiers.DIRTY_READ, QueryResultTypeInternal.EXTERNAL_ENTRY, false);

        Object returnObject = entry;

        try {
            returnObject =
                    ((ISpaceProxy) spaceProxy).getDirectProxy().getTypeManager().getObjectFromIGSEntry(entry);
        } catch (Exception exc) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(exc.toString(), exc);
            }
        }

        return returnObject;
    }

    public static void throwInternalSpaceException(String msg, Exception cause) throws InternalSpaceException {
        throw new InternalSpaceException(msg, cause);
    }

    public static void throwLeaseInternalSpaceException(String msg, Exception cause) throws InternalSpaceException {
        throw new LeaseInternalSpaceException(msg, cause);
    }

    public static void throwEngineInternalSpaceException(String msg, Exception cause) throws InternalSpaceException {
        throw new EngineInternalSpaceException(msg, cause);
    }

    public static ProxyInternalSpaceException createProxyInternalSpaceException(Exception e) throws InternalSpaceException {
        return new ProxyInternalSpaceException(e.getMessage(), e.getCause());
    }

    public static void throwReplicationInternalSpaceException(String msg, Exception cause) throws InternalSpaceException {
        throw new ReplicationInternalSpaceException(msg, cause);
    }

    /**
     * @return countSpaceObjects objects count calculated from passed SpaceRuntimeInfo instance
     */
    public static long countSpaceObjects(SpaceRuntimeInfo info) {
        long count = 0;
        int listSize = info.m_NumOFEntries.size();
        for (int i = 0; i < listSize; i++) {
            Integer intObj = info.m_NumOFEntries.get(i);
            count += intObj.intValue();
        }

        return count;
    }

    /**
     * Returns pair of String objects ( packed in as String array ) that are deparated by one
     * delimeter character in passed source String object. If delimeter is not found array with one
     * source String object is returned.
     *
     * @param source    string with only one delimeter character
     * @param delimeter delimeter character
     * @return array of split strings
     */
    public static String[] splitString(String source, char delimeter) {
        String[] resultArray = null;
        int delimeterIndex = source.indexOf(delimeter);
        //if ther is no delimeter in source string
        //in such case return source string itself within array
        if (delimeterIndex < 0) {
            resultArray = new String[1];
            resultArray[0] = source;
        } else {
            String firstStr = source.substring(0, delimeterIndex);
            String secondStr = source.substring(delimeterIndex + 1);
            resultArray = new String[2];
            resultArray[0] = firstStr;
            resultArray[1] = secondStr;
        }

        return resultArray;
    }

    /**
     * Create full space name based on container nadm and space name
     *
     * @return full space name: <ContainerName:SpaceName>
     */
    public static String createFullSpaceName(String containerName, String spaceName) {
        return containerName + ":" + spaceName;
    }

    /**
     * Format ms interval to min/sec/ms
     */
    static public String formatMillis(long millis) {
        boolean isMinutes = false;
        double instTime = millis;
        String suffix = "milliseconds";
        if (instTime > (60 * 1000)) {
            suffix = "minutes";
            instTime = instTime / (60 * 1000);
            isMinutes = true;
        } else if (instTime > 1000) {
            suffix = "seconds";
            instTime = instTime / 1000;
        }
        if (!isMinutes) {
            NumberFormat nf = NumberFormat.getInstance();
            nf.setMaximumFractionDigits(2);
            return nf.format(instTime) + " " + suffix;
        }

        int minutes = (int) instTime;
        double seconds = (instTime - minutes) * 60;
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(2);
        nf.setMinimumIntegerDigits(2);
        return minutes + ":" + nf.format(seconds) + " " + suffix;
    }

    // parse LUS groups and return String array of LUS groups
    static public String[] parseLookupGroups(String lookupGroups) {
        // default groups
        String[] groups = {PUBLIC_GROUP};

        if (!lookupGroups.equalsIgnoreCase(PUBLIC_GROUP)) {
            String grl;

            // if groups = none LookupDiscovery = NO_GROUPS
            if (lookupGroups.equalsIgnoreCase(NONE_GROUP))
                groups = LookupGroups.none();
            else if (lookupGroups.equalsIgnoreCase(ALL_GROUP))
                groups = LookupGroups.all();
            else {
                StringTokenizer st = new StringTokenizer(lookupGroups, ",");
                groups = new String[st.countTokens()];

                for (int i = 0; st.hasMoreTokens(); i++) {
                    grl = st.nextToken().trim();

                    // if groups = "all" and "none" the default is "all"
                    if (grl.equalsIgnoreCase(ALL_GROUP)) {
                        groups = LookupGroups.all();
                        break;
                    }

                    // fill groups array
                    groups[i] = grl.equalsIgnoreCase(PUBLIC) ? PUBLIC_GROUP : grl;
                } /* for */
            } /* else  equalsIgnoreCase( ALL ) */
        } /* if !gr.equalsIgnoreCase("PUBLIC_GROUP") */

        return groups;
    }

    /**
     * Keys in this map are cluster member names.
     *
     * @param clusterPolicy clusterPolicy instance that represents cluster
     * @return mmap of CLusterMemberInfo class instances per specific ClusterPolicy instance.
     */
    public static Map<String, ClusterMemberInfo> getClusterMembersInfoMap(ClusterPolicy clusterPolicy) {
        Set<ClusterMemberInfo> clusterMemberInfoSet = getClusterMembersInfoSet(clusterPolicy);
        Map<String, ClusterMemberInfo> resultMap =
                new HashMap<>(clusterMemberInfoSet.size());

        Iterator<ClusterMemberInfo> iterator = clusterMemberInfoSet.iterator();
        while (iterator.hasNext()) {
            ClusterMemberInfo clusterMemberInfo = iterator.next();
            resultMap.put(clusterMemberInfo.memberName, clusterMemberInfo);
        }

        return resultMap;
    }

    public static Set<ClusterMemberInfo> getClusterMembersInfoSet(ClusterPolicy clusterPolicy) {
        Set<ClusterMemberInfo> clusterMembersSet = new HashSet<>();
        //get cluster policies
        FailOverPolicy failOverPolicy = clusterPolicy.m_FailOverPolicy;
        LoadBalancingPolicy loadBalancingPolicy = clusterPolicy.m_LoadBalancingPolicy;
        ReplicationPolicy replicationPolicy = clusterPolicy.m_ReplicationPolicy;


        List<SpaceURL> urlsList = null;
        List<String> groupMemberNames = null;
        if (failOverPolicy != null) {
            urlsList = failOverPolicy.failOverGroupMembersURLs;
            groupMemberNames = failOverPolicy.failOverGroupMembersNames;
            Set<ClusterMemberInfo> failOverURLsSet = transformToSet(urlsList, groupMemberNames);
            clusterMembersSet.addAll(failOverURLsSet);
        }

        if (loadBalancingPolicy != null) {
            urlsList = loadBalancingPolicy.loadBalanceGroupMembersURLs;
            groupMemberNames = loadBalancingPolicy.loadBalanceGroupMembersNames;
            Set<ClusterMemberInfo> loadBalancingURLsSet = transformToSet(urlsList, groupMemberNames);
            clusterMembersSet.addAll(loadBalancingURLsSet);
        }

        if (replicationPolicy != null) {
            urlsList = replicationPolicy.m_ReplicationGroupMembersURLs;
            groupMemberNames = replicationPolicy.m_ReplicationGroupMembersNames;
            Set<ClusterMemberInfo> replicationURLsSet = transformToSet(urlsList, groupMemberNames);
            clusterMembersSet.addAll(replicationURLsSet);
        }

        return clusterMembersSet;
    }

    private static Set<ClusterMemberInfo> transformToSet(List<SpaceURL> urlsList, List<String> memberNamesList) {
        if (urlsList == null)
            return (new HashSet<>(1));

        Set<ClusterMemberInfo> set = new HashSet<>(urlsList.size());
        int listSize = urlsList.size();
        for (int i = 0; i < listSize; i++)// String memberName : memberNamesList )
        {
            //create cluster member info with only two following parameters:
            //member name and member URL
            SpaceURL memberURL = urlsList.get(i);
            String memberName = memberNamesList.get(i);
            ClusterMemberInfo clusterMemberInfo =
                    new ClusterMemberInfo(memberName, memberURL, null, null, null, false);
            set.add(clusterMemberInfo);
        }

        return set;
    }

    public static String retriveHostName(NIOInfoProvider nioInfoProvider)
            throws RemoteException {
        NIODetails nioDetails = nioInfoProvider.getNIODetails();
        return nioDetails.getHostName();
    }

    public static String retriveHostAddress(NIOInfoProvider nioInfoProvider)
            throws RemoteException {
        NIODetails nioDetails = nioInfoProvider.getNIODetails();
        return nioDetails.getHostAddress();
    }

    /**
     * Get the String value found in the JMXConnection entry, or null if the attribute set does not
     * include a JMXConnection
     */
    public static String getJMXConnectionUrl(ServiceItem serviceItem) {
        JMXConnection jmxConnection = getJMXConnectionEntry(serviceItem.attributeSets);
        return jmxConnection == null ? null : jmxConnection.jmxServiceURL;
    }

    public static boolean isJMXRemotePortDefined(ServiceItem serviceItem) {
        Boolean isJmxRemoteAuthenticationRequired = false;
        if (serviceItem.service instanceof JVMInfoProvider) {

            JVMInfoProvider jvmInfoProvider = (JVMInfoProvider) serviceItem.service;
            ServiceID serviceId = serviceItem.serviceID;
            try {
                isJmxRemoteAuthenticationRequired = jmxRemotePortDefiedMap.get(serviceId);
                if (isJmxRemoteAuthenticationRequired == null) {
                    JVMDetails jvmDetails = jvmInfoProvider.getJVMDetails();
                    Map<String, String> vmSystemProperties = jvmDetails.getSystemProperties();
                    isJmxRemoteAuthenticationRequired =
                            vmSystemProperties.get(SystemProperties.JMX_REMOTE_PORT) != null &&
                                    vmSystemProperties.get(SystemProperties.JMX_REMOTE_AUTHENTICATE) != null &&
                                    Boolean.parseBoolean(vmSystemProperties.get(SystemProperties.JMX_REMOTE_AUTHENTICATE));

                    jmxRemotePortDefiedMap.put(serviceId, isJmxRemoteAuthenticationRequired);
                }
            } catch (RemoteException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(e.toString(), e);
                }
            }
        }
        return isJmxRemoteAuthenticationRequired;
    }


    public static boolean isJMXEnabled(JVMDetails jvmDetails) {
        Boolean isJmxEnabled = false;

        if (jvmDetails != null) {
            Map<String, String> vmSystemProperties = jvmDetails.getSystemProperties();
            String isJmxEnabledProp = vmSystemProperties.get(SystemProperties.JMX_ENABLED_PROP);
            if( isJmxEnabledProp != null ) {
                isJmxEnabled = Boolean.getBoolean( isJmxEnabledProp );
            }
        }
        return isJmxEnabled;
    }


    /**
     * Get the String value found in the JMXConnection entry, or null if the attribute set does not
     * include a JMXConnection
     */
    private static JMXConnection getJMXConnectionEntry(Entry[] attrs) {
        JMXConnection jmxConnection = null;
        for (int x = 0; x < attrs.length; x++) {
            if (attrs[x] instanceof JMXConnection) {
                jmxConnection = (JMXConnection) attrs[x];
                break;
            }
        }

        return jmxConnection;
    }


    public static String retriveClusterName(Entry[] attrs) {
        String clusterName = null;
        for (Entry entry : attrs) {
            if (entry instanceof ClusterName) {
                clusterName = ((ClusterName) entry).name;
                break;
            }
        }
        if (clusterName != null && clusterName.equals("NONE")) {
            clusterName = null;
        }

        return clusterName;
    }

    public static String retrieveName(Entry[] attrs) {
        String name = null;
        for (Entry entry : attrs) {
            if (entry instanceof Name) {
                name = ((Name) entry).name;
                break;
            }
        }
        if (name != null && name.equals("NONE")) {
            name = null;
        }

        return name;
    }

    public static boolean isJDK_1_4Runtime() {
        return System.getProperty("java.specification.version", "").startsWith("1.4");
    }

    public static String createServiceName(String serviceType, int agentID, long pid) {
        return createServiceName(serviceType, agentID, pid, null);
    }

    public static String createServiceName(
            String serviceType, int agentID, long pid, String[] zones) {
        String name1 = agentID < 0 ?
                serviceType.toLowerCase() : serviceType.toLowerCase() + "-" + agentID;

        String zonesRepresentation = getZonesStringRepresentation(zones);
        if (zonesRepresentation.length() > 0) {
            zonesRepresentation = "[" + zonesRepresentation + "]";
        }

        String name2 = pid < 0 ? name1 : name1 + "[" + pid + "]" + zonesRepresentation;
        return name2;
    }

    public static String getLookupGroupsStringRepresentation(String[] lookupGroups) {
        String mergedGroups = "";
        //merge with already defined in custom properties groups
        if (lookupGroups != null) {
            for (String prop : lookupGroups) {
                if (mergedGroups.length() == 0) {
                    mergedGroups = prop;
                } else {
                    mergedGroups = mergedGroups + "," + prop;
                }
            }
        } else//if null
        {
            mergedGroups = "[ALL]";
        }

        return mergedGroups;
    }

    public static String getLookupLocatorsStringRepresentation(LookupLocator[] lookupLocators) {
        String mergedLocators = "";
        //merge with already defined in custom properties groups
        if (lookupLocators != null) {
            for (LookupLocator lookupLocator : lookupLocators) {
                String locator = lookupLocator.getHost() + ":" + lookupLocator.getPort();
                if (mergedLocators.length() == 0) {
                    mergedLocators = locator;
                } else {
                    mergedLocators = mergedLocators + "," + locator;
                }
            }
        }

        return mergedLocators;
    }

    public static LookupLocator createLookupLocator( String loookupLocatorStr ){
        int port = -1;
        int portIndex = loookupLocatorStr.indexOf(":");
        if (portIndex == -1) {
            port = net.jini.discovery.Constants.getDiscoveryPort();
        }
        String host = (portIndex == -1 ? loookupLocatorStr : loookupLocatorStr.substring(0, portIndex));
        if (portIndex != -1) {
            String p = loookupLocatorStr.substring(portIndex + 1, loookupLocatorStr.length());
            try {
                port = new Integer(p).intValue();
            }
            catch (Throwable t) {
                //error
                port = net.jini.discovery.Constants.getDiscoveryPort();
            }
        }

        return new LookupLocator(host, port);
    }

    public static String getZonesStringRepresentation(String[] zones) {
        String mergedZones = "";

        if (zones == null || zones.length == 0) {
            return mergedZones;
        }

        //merge with already defined in custom properties groups
        if (zones != null) {
            for (String zone : zones) {
                if (mergedZones.length() == 0) {
                    mergedZones = zone;
                } else {
                    mergedZones = mergedZones + "," + zone;
                }
            }
        }

        return mergedZones;
    }

    public static Entry getServiceItemLookupAttributeName(ServiceItem serviceItem, Class<? extends Entry> entryClass) {

        Entry entryResult = null;
        Entry[] attributeSets = serviceItem.attributeSets;
        for (Entry entry : attributeSets) {
            if (entry.getClass().isAssignableFrom(entryClass)) {
                entryResult = entry;
                break;
            }
        }

        return entryResult;
    }

    public static String getTransactionTypeName(int transactionType) {
        switch (transactionType) {
            case TransactionInfo.Types.ALL:
                return "All";
            case TransactionInfo.Types.JINI:
                return "Distributed";
            case TransactionInfo.Types.LOCAL:
                return "Local";
            case TransactionInfo.Types.XA:
                return "XA";
        }

        return "";
    }

    public static String getTransactionStatusName(int transactionStatus) {
        switch (transactionStatus) {
            case TransactionConstants.ABORTED:
                return "Aborted";
            case TransactionConstants.ACTIVE:
                return "Active";
            case TransactionConstants.COMMITTED:
                return "Committed";
            case TransactionConstants.NOTCHANGED:
                return "Not changed";
            case TransactionConstants.PREPARED:
                return "Prepared";
            case TransactionConstants.VOTING:
                return "Voting";
        }

        return "";
    }


    public static String getTransactionLockTypeName(UnderTxnLockedObject lockedObject) {
        switch (lockedObject.getLockType()) {
            case UnderTxnLockedObject.LOCK_TYPE_READ_LOCK:
                return "Read Lock";
            case UnderTxnLockedObject.LOCK_TYPE_WRITE_LOCK:
                return "Write Lock";
        }
        return " - ";
    }

    public static String getTransactionLockOperationTypeName(UnderTxnLockedObject lockedObject) {

        switch (lockedObject.getOperationType()) {
            case SpaceOperations.NOTIFY:
                return "Notify";
            case SpaceOperations.READ:
                return "Read";
            case SpaceOperations.READ_IE:
                return "Read If Exists";
            case SpaceOperations.TAKE:
                return "Take";
            case SpaceOperations.TAKE_IE:
                return "Take If Exists";
            case SpaceOperations.UPDATE:
                return "Update";
            case SpaceOperations.WRITE:
                return "Write";
        }
        return " - ";
    }

    public static Properties parsePropertiesParam(String argsStr) {

        Properties props = new Properties();
        if (argsStr != null) {
            StringTokenizer tokenizer = new StringTokenizer(argsStr, ";");
            while (tokenizer.hasMoreTokens()) {
                String property = tokenizer.nextToken();
                int equalsIndex = property.indexOf('=');
                if (equalsIndex == -1) {
                    props.setProperty(property, "");
                } else {
                    props.setProperty(property.substring(0, equalsIndex),
                            property.substring(equalsIndex + 1));
                }
            }
        }

        return props.isEmpty() ? null : props;
    }

    /**
     * Get the codebase for the service by getting the service's classloader and extracting only the
     * http: adress and port number
     *
     * @return A String codebase
     */
    public static Set<String> getExportCodebasesSet(Object service) {
        URLClassLoader cl = (URLClassLoader) service.getClass().getClassLoader();
        URL[] urls = cl.getURLs();
        Set<String> codebaseSet = new HashSet<>(urls.length);
        for (URL url : urls) {
            String exportCodebase = retrieveCodebase(url);

            codebaseSet.add(exportCodebase);
        }

        return codebaseSet;
    }

    /**
     * Get the codebase for the service by getting the service's classloader and extracting only the
     * http: adress and port number
     *
     * @return A String codebase
     */
    public static String getExportCodebase(Object service) {
        URLClassLoader cl = (URLClassLoader) service.getClass().getClassLoader();
        URL[] urls = cl.getURLs();

        return retrieveCodebase(urls[0]);
    }

    private static String retrieveCodebase(URL url) {
        String exportCodebase = null;
        try {
            exportCodebase = url.toURI().getPath();
        } catch (URISyntaxException e) {
            if (_logger.isWarnEnabled()) {
                _logger.warn(e.toString(), e);
            }
        }

        if (exportCodebase == null) {
            return "";
        }

        if (exportCodebase.indexOf(".jar") != -1) {
            int index = exportCodebase.lastIndexOf('/');
            if (index != -1)
                exportCodebase = exportCodebase.substring(0, index + 1);
        }

        /*
         * TODO: If the exportCodebase starts with httpmd, replace
         * httpmd with http. Need to figure out a mechanism to use the
         * httpmd in a better way
         */
        if (exportCodebase.startsWith("httpmd")) {
            exportCodebase = "http" + exportCodebase.substring(6);
        }

        return exportCodebase;
    }


    // All schemas except mirror
    private static final Set<String> ALL_SPACE_SCHEMAS = CollectionUtils.toUnmodifiableSet(
            Constants.Schemas.DEFAULT_SCHEMA,
            Constants.Schemas.JAVASPACE_SCHEMA,
            Constants.Schemas.CACHE_SCHEMA, Constants.Schemas.PERSISTENT_SCHEMA);

    public static Set<String> getSpaceSchemas() {
        return ALL_SPACE_SCHEMAS;
    }

    public static Integer[] getArrayValuesIndexes(short[] array, Short... checkedValues ){
        List<Short> checkedValuesList = Arrays.asList(checkedValues);
        List<Integer> foundIndexes = new ArrayList<>();
        for( int i = 0; i < array.length; i++ ){
            short arrayElement = array[ i ];
            if( checkedValuesList.contains( arrayElement ) ){
                foundIndexes.add( i );
                break;
            }
        }

        return foundIndexes.toArray( new Integer[0] );
    }

    public static boolean areAllArrayElementsNull( Object[] array ){
        boolean hasAllElementsAsNulls = true;
        for ( Object obj : array ) {
            if (obj != null) {
                hasAllElementsAsNulls = false;
                break;
            }
        }

        return hasAllElementsAsNulls;
    }
}
