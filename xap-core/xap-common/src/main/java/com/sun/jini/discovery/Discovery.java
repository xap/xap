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

package com.sun.jini.discovery;

import net.jini.core.constraint.InvocationConstraints;
import net.jini.io.UnsupportedConstraintException;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.Socket;
import java.util.Collection;

/**
 * Class providing methods for participating in versions 1 and 2 of the discovery protocol. <p>
 *
 * <b><font size="+1">Logging</font></b> <p>
 *
 * This implementation uses the {@link java.util.logging.Logger}s named
 * <code>com.sun.jini.discovery.DiscoveryV1</code> and <code>com.sun.jini.discovery.DiscoveryV2</code>
 * to log information at the following logging levels: <p>
 *
 * <table border="1" cellpadding="5" summary="Describes logging performed by the Discovery class to
 * the DiscoveryV1 logger at various logging levels">
 *
 * <caption halign="center" valign="top"><b><code> com.sun.jini.discovery.DiscoveryV1</code></b></caption>
 *
 * <tr> <th scope="col"> Level <th scope="col"> Description
 *
 * <tr> <td> {@link java.util.logging.Level#FINEST FINEST} <td> Encoding and decoding of discovery
 * protocol version 1 multicast requests, multicast announcements, and unicast responses
 *
 * </table> <p>
 *
 * <table border="1" cellpadding="5" summary="Describes logging performed by the Discovery class to
 * the DiscoveryV2 logger at various logging levels">
 *
 * <caption halign="center" valign="top"><b><code> com.sun.jini.discovery.DiscoveryV2</code></b></caption>
 *
 * <tr> <th scope="col"> Level <th scope="col"> Description
 *
 * <tr> <td> {@link java.util.logging.Level#WARNING WARNING} <td> Truncation of unicast request
 * format ID list due to length; discovery format providers that are unusable or have conflicting
 * discovery format IDs
 *
 * <tr> <td> {@link java.util.logging.Level#FINE FINE} <td> Constraint check failures
 * encountered during the unicast discovery handshake when determining a suitable discovery format
 * to use
 *
 * <tr> <td> {@link java.util.logging.Level#FINEST FINEST} <td> Encoding and decoding of discovery
 * protocol version 2 multicast requests, multicast announcements, and unicast responses; also,
 * access of <code>Discovery</code> instances implementing protocol version 2
 *
 * </table>
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
public abstract class Discovery {

    /**
     * The version number for discovery protocol version 1.
     */
    public static final int PROTOCOL_VERSION_1 = 1;

    /**
     * The version number for discovery protocol version 2.
     */
    public static final int PROTOCOL_VERSION_2 = 2;

    static final int MIN_MAX_PACKET_SIZE = 512;

    /**
     * Returns an instance implementing protocol version 1.
     *
     * @return an instance implementing protocol version 1
     */
    public static Discovery getProtocol1() {
        return DiscoveryV1.getInstance();
    }

    /**
     * Returns an instance implementing protocol version 2 which uses providers loaded from the
     * given class loader, or the current context class loader if the given loader is
     * <code>null</code>.  Available providers are determined by interpreting any resources of the
     * indicated class loader named <code>"META-INF/services/com.sun.jini.discovery.DiscoveryFormatProvider"</code>
     * as files containing names of provider classes, with one name per line.
     *
     * @param loader class loader from which to load providers, or <code>null</code> to indicate the
     *               current context class loader
     * @return an instance implementing protocol version 2
     */
    public static Discovery getProtocol2(ClassLoader loader) {
        return DiscoveryV2.getInstance(loader);
    }

    /**
     * Returns an instance implementing protocol version 2 which uses the given providers.  Contents
     * of arrays are copied; <code>null</code> array values are equivalent to empty arrays.
     *
     * @param mre providers for encoding multicast requests
     * @param mrd providers for decoding multicast requests
     * @param mae providers for encoding multicast announcements
     * @param mad providers for decoding multicast announcements
     * @param udc providers for performing the client side of unicast discovery
     * @param uds providers for performing the server side of unicast discovery
     * @return an instance implementing protocol version 2
     */
    public static Discovery getProtocol2(MulticastRequestEncoder[] mre,
                                         MulticastRequestDecoder[] mrd,
                                         MulticastAnnouncementEncoder[] mae,
                                         MulticastAnnouncementDecoder[] mad,
                                         UnicastDiscoveryClient[] udc,
                                         UnicastDiscoveryServer[] uds) {
        return DiscoveryV2.getInstance(mre, mrd, mae, mad, udc, uds);
    }

    /**
     * Returns an iterator which can be used to encode the given multicast request data into sets of
     * {@link DatagramPacket}s, each bounded in length by the specified maximum packet size, in a
     * manner that satisfies the given constraints.  <code>null</code> constraints are considered
     * equivalent to empty constraints.  The destination of each <code>DatagramPacket</code>
     * produced by the returned iterator is set to the address returned by {@link
     * net.jini.discovery.Constants#getRequestAddress}, with the value of {@link
     * net.jini.discovery.Constants#multicastDiscoveryPort} used as the destination port.
     *
     * @param request       the request data to encode
     * @param maxPacketSize the maximum size of packets to produce
     * @param constraints   the constraints to apply when encoding the data, or <code>null</code>
     * @return an iterator to use for encoding the data
     * @throws NullPointerException if <code>request</code> is <code>null</code>
     */
    public abstract EncodeIterator encodeMulticastRequest(
            MulticastRequest request,
            int maxPacketSize,
            InvocationConstraints constraints);

    /**
     * Decodes the multicast request data contained in the given datagram in a manner that satisfies
     * the specified constraints and client subject checker (if any), returning a {@link
     * MulticastRequest} instance that contains the decoded data.  <code>null</code> constraints are
     * considered equivalent to empty constraints.  All the specified constraints are checked before
     * this method returns.
     *
     * @param packet      the packet to decode
     * @param constraints the constraints to apply when decoding the packet, or <code>null</code>
     * @param checker     the object to use to check the client subject, or <code>null</code>
     * @return the decoded multicast request data
     * @throws IOException                    if an error occurs in interpreting the data
     * @throws UnsupportedConstraintException if unable to satisfy the specified constraints
     * @throws SecurityException              if the given constraints cannot be satisfied due to
     *                                        insufficient caller permissions, or if the client
     *                                        subject check fails
     * @throws NullPointerException           if <code>packet</code> is <code>null</code>
     */
    public abstract MulticastRequest decodeMulticastRequest(
            DatagramPacket packet,
            InvocationConstraints constraints,
            ClientSubjectChecker checker)
            throws IOException;

    /**
     * Decodes the multicast request data contained in the given datagram in a manner that satisfies
     * the specified constraints and client subject checker (if any), returning a {@link
     * MulticastRequest} instance that contains the decoded data, with constraint checking
     * optionally delayed.  <code>null</code> constraints are considered equivalent to empty
     * constraints. <p>The <code>delayConstraintCheck</code> flag is used to control delayed
     * constraint checking. Delayed constraint checking is useful for potentially delaying the
     * expense of complete constraint checking, until other checks have been made on the returned
     * <code>MulticastRequest</code> for preliminary validation. Implementations may ignore the
     * flag, in which case, the behavior is equivalent to that of {@link
     * Discovery#decodeMulticastRequest(DatagramPacket, InvocationConstraints, ClientSubjectChecker)
     * decodeMulticastRequest}. <p>If <code>delayConstraintCheck</code> is <code>true</code>, the
     * method behaves as follows:<ul> <li> Some of the specified constraints may not be checked
     * before this method returns; the returned <code>MulticastRequest</code>'s {@link
     * MulticastRequest#checkConstraints checkConstraints} method must be invoked to complete
     * checking of all the constraints. <li> Constraints which must be checked before accessor
     * methods of the returned <code>MulticastRequest</code> can be invoked are always checked
     * before this method returns.</ul> <p>If <code>delayConstraintCheck</code> is
     * <code>false</code>, all the specified constraints are checked before this method returns.
     * <p><code>Discovery</code> implements this method to simply invoke {@link
     * Discovery#decodeMulticastRequest(DatagramPacket, InvocationConstraints, ClientSubjectChecker)
     * decodeMulticastRequest}, and thus checks all the specified constraints before returning.
     *
     * @param packet               the packet to decode
     * @param constraints          the constraints to apply when decoding the packet, or
     *                             <code>null</code>
     * @param checker              the object to use to check the client subject, or
     *                             <code>null</code>
     * @param delayConstraintCheck flag to control delayed constraint checking
     * @return the decoded multicast request data.
     * @throws IOException          if an error occurs in interpreting the data
     * @throws SecurityException    if the given constraints cannot be satisfied due to insufficient
     *                              caller permissions, or if the client subject check fails
     * @throws NullPointerException if <code>packet</code> is <code>null</code>
     * @since 2.1
     */
    public MulticastRequest decodeMulticastRequest(
            DatagramPacket packet,
            InvocationConstraints constraints,
            ClientSubjectChecker checker,
            boolean delayConstraintCheck)
            throws IOException {
        return decodeMulticastRequest(packet, constraints, checker);
    }

    /**
     * Returns an iterator which can be used to encode the given multicast announcement data into
     * sets of {@link DatagramPacket}s, each bounded in length by the specified maximum packet size,
     * in a manner that satisfies the given constraints.  <code>null</code> constraints are
     * considered equivalent to empty constraints.  The destination of each
     * <code>DatagramPacket</code> produced by the returned iterator is set to the address returned
     * by {@link net.jini.discovery.Constants#getAnnouncementAddress}, with the value of {@link
     * net.jini.discovery.Constants#multicastDiscoveryPort} used as the destination port.
     *
     * @param announcement  the announcement data to encode
     * @param maxPacketSize the maximum size of packets to produce
     * @param constraints   the constraints to apply when encoding the data, or <code>null</code>
     * @return an iterator to use for encoding the data
     * @throws NullPointerException if <code>announcement</code> is <code>null</code>
     */
    public abstract EncodeIterator encodeMulticastAnnouncement(
            MulticastAnnouncement announcement,
            int maxPacketSize,
            InvocationConstraints constraints);

    /**
     * Decodes the multicast announcement data contained in the given datagram in a manner that
     * satisfies the specified constraints, returning a {@link MulticastAnnouncement} instance that
     * contains the decoded data. <code>null</code> constraints are considered equivalent to empty
     * constraints.  All the specified constraints are checked before this method returns.
     *
     * @param packet      the packet to decode
     * @param constraints the constraints to apply when decoding the packet, or <code>null</code>
     * @return the decoded multicast announcement data
     * @throws IOException                    if an error occurs in interpreting the data
     * @throws UnsupportedConstraintException if unable to satisfy the specified constraints
     * @throws SecurityException              if the given constraints cannot be satisfied due to
     *                                        insufficient caller permissions
     * @throws NullPointerException           if <code>packet</code> is <code>null</code>
     */
    public abstract MulticastAnnouncement decodeMulticastAnnouncement(
            DatagramPacket packet,
            InvocationConstraints constraints)
            throws IOException;

    /**
     * Decodes the multicast announcement data contained in the given datagram in a manner that
     * satisfies the specified constraints, returning a {@link MulticastAnnouncement} instance that
     * contains the decoded data, with constraint checking optionally delayed.  <code>null</code>
     * constraints are considered equivalent to empty constraints. <p>The
     * <code>delayConstraintCheck</code> flag is used to control delayed constraint checking.
     * Delayed constraint checking is useful for potentially delaying the expense of complete
     * constraint checking, until other checks have been made on the returned
     * <code>MulticastAnnouncement</code> for preliminary validation. Implementations may ignore the
     * flag, in which case, the behavior is equivalent to that of {@link
     * Discovery#decodeMulticastAnnouncement(DatagramPacket, InvocationConstraints)
     * decodeMulticastAnnouncement}. <p>If <code>delayConstraintCheck</code> is <code>true</code>,
     * the method behaves as follows:<ul> <li> Some of the specified constraints may not be checked
     * before this method returns; the returned <code>MulticastAnnouncement</code>'s {@link
     * MulticastAnnouncement#checkConstraints checkConstraints} method must be invoked to complete
     * checking of all the constraints. <li> Constraints which must be checked before accessor
     * methods of the returned <code>MulticastAnnouncement</code> can be invoked are always checked
     * before this method returns.</ul> <p> If <code>delayConstraintCheck</code> is
     * <code>false</code>, all the specified constraints are checked before this method returns.
     * <p><code>Discovery</code> implements this method to simply invoke {@link
     * Discovery#decodeMulticastAnnouncement(DatagramPacket, InvocationConstraints)
     * decodeMulticastAnnouncement}, and thus checks all the specified constraints before
     * returning.
     *
     * @param packet               the packet to decode
     * @param constraints          the constraints to apply when decoding the packet, or
     *                             <code>null</code>
     * @param delayConstraintCheck flag to control delayed constraint checking
     * @return the decoded multicast announcement data.
     * @throws IOException                    if an error occurs in interpreting the data
     * @throws UnsupportedConstraintException if unable to satisfy the specified constraints
     * @throws SecurityException              if the given constraints cannot be satisfied due to
     *                                        insufficient caller permissions.
     * @throws NullPointerException           if <code>packet</code> is <code>null</code>
     * @since 2.1
     */
    public MulticastAnnouncement decodeMulticastAnnouncement(
            DatagramPacket packet,
            InvocationConstraints constraints,
            boolean delayConstraintCheck)
            throws IOException {
        return decodeMulticastAnnouncement(packet, constraints);
    }

    /**
     * Performs the client side of unicast discovery, obtaining the returned response data over the
     * provided socket using the given default and codebase verifier class loaders and collection of
     * object stream context objects in a manner that satisfies the specified constraints.
     * <code>null</code> constraints are considered equivalent to empty constraints.
     *
     * @param socket         the socket on which to perform unicast discovery
     * @param constraints    the constraints to apply to unicast discovery, or <code>null</code>
     * @param defaultLoader  the class loader value (possibly <code>null</code>) to be passed as the
     *                       <code>defaultLoader</code> argument to <code>RMIClassLoader</code>
     *                       methods when unmarshalling the registrar proxy
     * @param verifierLoader the class loader value (possibly <code>null</code>) to pass to {@link
     *                       net.jini.security.Security#verifyCodebaseIntegrity
     *                       Security.verifyCodebaseIntegrity}, if codebase integrity verification
     *                       is used when unmarshalling the registrar proxy
     * @param context        the collection of context information objects (possibly
     *                       <code>null</code>) to use when unmarshalling the registrar proxy
     * @return the received unicast response data
     * @throws IOException                    if an error occurs in interpreting received data or in
     *                                        formatting data to send
     * @throws UnsupportedConstraintException if unable to satisfy the specified constraints
     * @throws SecurityException              if the given constraints cannot be satisfied due to
     *                                        insufficient caller permissions
     * @throws ClassNotFoundException         if the class of the discovered registrar cannot be
     *                                        resolved
     * @throws NullPointerException           if <code>socket</code> is <code>null</code>
     */
    public abstract UnicastResponse doUnicastDiscovery(
            Socket socket,
            InvocationConstraints constraints,
            ClassLoader defaultLoader,
            ClassLoader verifierLoader,
            Collection context)
            throws IOException, ClassNotFoundException;

    /**
     * Handles the server side of unicast discovery, transmitting the given response data over the
     * provided socket using the given collection of object stream context objects in a manner that
     * satisfies the specified constraints and client subject checker (if any).  This method assumes
     * that the protocol version number has already been consumed from the socket, but that no
     * further processing of the connection has occurred. <code>null</code> constraints are
     * considered equivalent to empty constraints.
     *
     * @param response    the unicast response data to transmit
     * @param socket      the socket on which to handle unicast discovery
     * @param constraints the constraints to apply to unicast discovery, or <code>null</code>
     * @param checker     the object to use to check the client subject, or <code>null</code>
     * @param context     the collection of context information objects to use when marshalling the
     *                    registrar proxy
     * @throws IOException                    if the protocol handshake fails, or if an error occurs
     *                                        in interpreting received data or in formatting data to
     *                                        send
     * @throws UnsupportedConstraintException if unable to satisfy the specified constraints
     * @throws SecurityException              if the given constraints cannot be satisfied due to
     *                                        insufficient caller permissions, or if the client
     *                                        subject check fails
     * @throws NullPointerException           if <code>response</code>, <code>socket</code>, or
     *                                        <code>context</code> is <code>null</code>
     */
    public abstract void handleUnicastDiscovery(
            UnicastResponse response,
            Socket socket,
            InvocationConstraints constraints,
            ClientSubjectChecker checker,
            Collection context)
            throws IOException;
}
