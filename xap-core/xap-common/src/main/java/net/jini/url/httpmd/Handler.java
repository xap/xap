/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package net.jini.url.httpmd;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stream handler for URLs with the HTTPMD protocol. HTTPMD URLs provide a way to insure the
 * integrity of data retrieved from an HTTP URL. The HTTPMD URL includes a message digest for the
 * data to be retrieved from the URL. The URL input stream insures that the data has the correct
 * message digest when the end of file for the stream is reached. If the data has the wrong message
 * digest, a {@link WrongMessageDigestException} is thrown. <p>
 *
 * HTTPMD URLs may be used to guarantee the integrity of a downloaded object's codebase if used when
 * specifying the URLs for the JAR files containing the object's classes. Because HTTPMD URLs
 * specify a message digest for a single item, they should not be used for directories of classes.
 * <p>
 *
 * HTTPMD URLs have a syntax similar to that of HTTP URLs, but include a message digest as the last
 * parameter stored in the last segment of the path. The parameter is introduced by the
 * '<code>;</code>' character, and includes the name of the message digest algorithm, a
 * '<code>=</code>', the message digest, and an optional comment introduced by the '<code>,</code>'
 * character. In addition, a comment by itself may be specified in a relative HTTPMD URL. Comments
 * are ignored when using <code>equals</code> to compare HTTPMD URLs. The comment specified in the
 * context URL is ignored when parsing a relative HTTPMD URL. Adding a comment to an HTTPMD URL is
 * useful in cases where the URL is required to have a particular suffix, for example the ".jar"
 * file extension. A comment-only relative HTTPMD URL is useful when specifying the URL of the
 * containing document from within the contents of the document, where the message digest cannot be
 * specified because it is not yet known. <p>
 *
 * The message digest algorithm is case-insensitive, and may include ASCII letters and numbers, as
 * well as the following characters:
 *
 * <pre>
 * - _ . ~ * ' ( ) : @ & + $ ,
 * </pre> <p>
 *
 * The value specifies the name of the {@link MessageDigest} algorithm to use. For the URL syntax to
 * be valid, the value must be the name of a <code>MessageDigest</code> algorithm as determined by
 * calling {@link MessageDigest#getInstance(String)}. <p>
 *
 * The message digest is represented as a positive hexadecimal integer, using digits, and the
 * letters '<code>a</code>' through '<code>f</code>', in either lowercase or uppercase. <p>
 *
 * The characters following the '<code>,</code>' comment character may include ASCII letters and
 * numbers, as well as the following characters:
 *
 * <pre>
 * - _ . ~ * ' ( ) : @ & = + $ ,
 * </pre> <p>
 *
 * Here are some examples of HTTPMD URLs: <p> <ul>
 *
 * <li> An absolute URL: <br> <code> httpmd://www.sun.com/index.html;md5=7be207c7111e459eeea1c9b3d04f1667
 * </code>
 *
 * <li> A relative URL: <br> <code>index.html;sha=99f6837808c0a79398bf69d83cfb1b82d20cf0cf,Comment
 * </code>
 *
 * <li> A comment-only relative URL: <br> <code>,.jar</code> </ul>
 *
 * @author Sun Microsystems, Inc.
 * @see HttpmdUtil
 * @since 2.0
 *
 *
 * This implementation of HTTPMD URLs uses the {@link Logger} named <code>net.jini.url.httpmd</code>
 * to log information at the following logging levels: <p>
 *
 * <table border="1" cellpadding="5" summary="Describes logging performed by the HTTPMD URL handler
 * at different logging levels">
 *
 * <caption halign="center" valign="top"><b><code> net.jini.url.httpmd</code></b></caption>
 *
 * <tr> <th scope="col"> Level <th scope="col"> Description
 *
 * <tr> <td> {@link Level#FINE FINE} <td> URL input stream detects an incorrect message digest
 *
 * </table> <p>
 */
@com.gigaspaces.api.InternalApi
public class Handler extends URLStreamHandler {

    /**
     * Creates a URL stream handler for HTTPMD URLs.
     */
    public Handler() {
    }

    /**
     * Returns the default port for a URL parsed by this handler, which is <code>80</code>.
     *
     * @return the default port for a URL parsed by this handler
     */
    protected int getDefaultPort() {
        return 80;
    }

    /**
     * Creates a HTTP URL connection for an HTTPMD URL.
     */
    protected URLConnection openConnection(URL u) throws IOException {
        return new HttpmdURLConnection(u);
    }

    /**
     * Parses the string representation of an HTTPMD URL object.
     *
     * @throws IllegalArgumentException if the URL is malformed
     */
    protected void parseURL(URL url, String spec, int start, int limit) {
    /* Check for a relative URL that only specifies a comment */
        if (start < limit && spec.charAt(start) == ',') {
            String query = url.getQuery();
            int queryStart = spec.indexOf('?', start);
            if (queryStart != -1) {
                query = spec.substring(queryStart + 1, limit);
                limit = queryStart;
            }
            String path = url.getPath() == null ? "" : url.getPath();
        /* Remove the comment from the context path */
            int param = path.lastIndexOf(';');
            if (param != -1) {
                int comment = path.indexOf(',', param);
                if (comment != -1) {
                    path = path.substring(0, comment);
                }
            }
	    /* Append the new comment */
            path += spec.substring(start, limit);
            setURL(url, url.getProtocol(), url.getHost(), url.getPort(),
                    url.getAuthority(), url.getUserInfo(), path, query,
                    url.getRef());
        } else {
	    /* Otherwise, combine spec with the context URL */
            super.parseURL(url, spec, start, limit);
        }

	/* Check syntax of message digest parameter */
        String path = url.getPath() == null ? "" : url.getPath();
        int semiIndex = path.lastIndexOf(';');
        if (semiIndex < 0) {
            throw new IllegalArgumentException(
                    "Message digest parameter is missing");
        }
        int equalsIndex = path.indexOf('=', semiIndex);
        if (equalsIndex < 0) {
            throw new IllegalArgumentException(
                    "Message digest parameter is missing a '='");
        }
        String algorithm = path.substring(semiIndex + 1, equalsIndex);
        try {
            MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(
                    "Message digest parameter algorithm is not found: " +
                            algorithm);
        }
        String digest = path.substring(equalsIndex + 1);
        int comment = digest.indexOf(',');
        if (comment != -1) {
            for (int i = digest.length(); --i > comment; ) {
                char c = digest.charAt(i);
                if (!HttpmdUtil.commentChar(c)) {
                    throw new IllegalArgumentException(
                            "Comment contains illegal character: '" + c + "'");
                }
            }
            digest = digest.substring(0, comment);
        }
        int length = digest.length();
        if (length == 0) {
            throw new IllegalArgumentException(
                    "Message digest parameter digest is empty");
        }
        for (int i = length; --i >= 0; ) {
            char c = digest.charAt(i);
            if (Character.digit(c, 16) < 0) {
                throw new IllegalArgumentException(
                        "Message digest parameter has invalid hex character: " +
                                c);
            }
        }
    }

    /**
     * Compares two HTTPMD URLs to see if they refer to the same file. Performs case-insensitive
     * comparison of the protocols and of the message digest parameters (ignoring the comment),
     * calls <code>hostsEqual</code> to compare the hosts, compares the ports, with <code>-1</code>
     * matching the default HTTP port (<code>80</code>), and performs case-sensitive comparison on
     * the remaining portions.
     */
    protected boolean sameFile(URL u1, URL u2) {
        /* Compare the protocols */
        if (!((u1.getProtocol() == u2.getProtocol()) ||
                (u1.getProtocol() != null &&
                        u1.getProtocol().equalsIgnoreCase(u2.getProtocol())))) {
            return false;
        }
	/* Compare the hosts */
        if (!hostsEqual(u1, u2)) {
            return false;
        }
	/* Compare the paths */
        String path1 = u1.getPath();
        String path2 = u2.getPath();
        if (path1 == path2) {
	    /* Paths are OK */
        } else if (path1 == null || path2 == null) {
            return false;
        } else if (!path1.equals(path2)) {
	    /*
	     * Perform case insensitive matching on the message digest
	     * parameters, ignoring comments.
	     */
            int param = path1.lastIndexOf(';');
            if (param < 0 || param != path2.lastIndexOf(';')) {
                return false;
            }
	    /* Case-sensitive match for non-parameter part */
            if (!path1.regionMatches(0, path2, 0, param)) {
                return false;
            }
	    /* Ignore comments */
            int comment1 = path1.indexOf(',', param + 1);
            int len = (comment1 != -1) ? comment1 : path1.length();
            int comment2 = path2.indexOf(',', param + 1);
            int len2 = (comment2 != -1) ? comment2 : path2.length();
            if (len != len2) {
                return false;
            }
	    /* Case-insensitive match for algorithm and digest */
            if (!path1.regionMatches(true, param + 1,
                    path2, param + 1, len - param - 1)) {
                return false;
            }
        }
	/* Compare queries */
        if (u1.getQuery() == null
                ? u2.getQuery() != null
                : !u1.getQuery().equals(u2.getQuery())) {
            return false;
        }
	/* Compare the ports */
        int port1 = (u1.getPort() != -1) ? u1.getPort() : u1.getDefaultPort();
        int port2 = (u2.getPort() != -1) ? u2.getPort() : u2.getDefaultPort();
        if (port1 != port2) {
            return false;
        }
        return true;
    }

    /**
     * Computes the hash code for the specified URL. This method ignores the comment portion of the
     * message digest parameter, and ignores the case of characters in the message digest and
     * algorithm.
     */
    protected int hashCode(URL u) {
        int h = 0;

        /* Generate the protocol part */
        String protocol = u.getProtocol();
        if (protocol != null) {
            h += protocol.hashCode();
        }

        /* Generate the host part */
        InetAddress addr = getHostAddress(u);
        if (addr != null) {
            h += addr.hashCode();
        } else {
            String host = u.getHost();
            if (host != null) {
                h += host.toLowerCase().hashCode();
            }
        }

	/*
	 * Generate the path part, ignoring case in the message digest and
	 * algorithm, and ignoring comment.
	 */
        String path = u.getPath();
        if (path != null) {
            int param = path.lastIndexOf(';');
            if (param == -1) {
                h += path.hashCode();
            } else {
                h += path.substring(0, param).hashCode();
                int comment = path.indexOf(',', param);
                if (comment != -1) {
                    path = path.substring(0, comment);
                }
                h += path.substring(param).toLowerCase().hashCode();
            }
        }

        /* Generate the query part */
        String query = u.getQuery();
        if (query != null) {
            h += query.hashCode();
        }

        /* Generate the port part */
        if (u.getPort() == -1) {
            h += getDefaultPort();
        } else {
            h += u.getPort();
        }

        /* Generate the ref part */
        String ref = u.getRef();
        if (ref != null)
            h += ref.hashCode();

        return h;
    }
}
