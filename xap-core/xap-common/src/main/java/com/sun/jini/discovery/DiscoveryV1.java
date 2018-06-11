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

import com.sun.jini.discovery.internal.Plaintext;

import net.jini.core.constraint.InvocationConstraints;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.discovery.Constants;
import net.jini.io.MarshalInputStream;
import net.jini.io.OptimizedByteArrayInputStream;
import net.jini.io.UnsupportedConstraintException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class providing methods for implementing discovery protocol version 1.
 */
class DiscoveryV1 extends Discovery {

    private static final int SERVICE_ID_LEN = 16;

    private static final byte REQUEST_TYPE = (byte) 1;
    private static final byte ANNOUNCEMENT_TYPE = (byte) 2;

    private static final DiscoveryV1 instance = new DiscoveryV1();
    private static final Logger logger =
            Logger.getLogger(DiscoveryV1.class.getName());

    static DiscoveryV1 getInstance() {
        return instance;
    }

    public EncodeIterator encodeMulticastRequest(
            final MulticastRequest request,
            final int maxPacketSize,
            final InvocationConstraints constraints) {
        if (maxPacketSize < MIN_MAX_PACKET_SIZE) {
            throw new IllegalArgumentException("maxPacketSize too small");
        }
        return new EncodeIterator() {

            private boolean used;

            public DatagramPacket[] next() throws IOException {
                used = true;
                return encode(request, maxPacketSize, constraints);
            }

            public boolean hasNext() {
                return !used;
            }
        };
    }

    private static DatagramPacket[] encode(MulticastRequest request,
                                           int maxPacketSize,
                                           InvocationConstraints constraints)
            throws IOException {
        final int MIN_DATA_LEN = 16;
        final int NUM_GROUPS_LEN = 4;
        final int NUM_SERVICE_IDS_LEN = 4;

        checkConstraints(constraints);

        // precompute length of UTF-encoded group names
        LinkedList groups = new LinkedList();
        byte[] host = Plaintext.toUtf(request.getHost());
        String[] g = request.getGroups();
        for (int i = 0; i < g.length; i++) {
            byte[] b = Plaintext.toUtf(g[i]);
            if (b.length + host.length + MIN_DATA_LEN > maxPacketSize) {
                throw new DiscoveryProtocolException(
                        "group too long: " + g[i]);
            }
            groups.add(b);
        }

        List packets = new ArrayList();
        do {
            ByteBuffer buf = ByteBuffer.allocate(maxPacketSize);

            // write protocol version
            buf.putInt(PROTOCOL_VERSION_1);
            buf.put(REQUEST_TYPE);

            // write client port
            buf.putInt(request.getPort());

            // write known service IDs
            ServiceID[] ids = request.getServiceIDs();
            int nids = Math.min(
                    ids.length,
                    (buf.remaining()
                            - (groups.isEmpty() ? 0 : ((byte[]) groups.getFirst()).length)
                            - NUM_SERVICE_IDS_LEN - NUM_GROUPS_LEN) / SERVICE_ID_LEN);
            buf.putInt(nids);
            for (int i = 0; i < nids; i++) {
                buf.putLong(ids[i].getMostSignificantBits());
                buf.putLong(ids[i].getLeastSignificantBits());
            }

            // write lookup groups
            int ngroupsPos = buf.position();
            int ngroups = 0;
            buf.putInt(ngroups);
            while (!groups.isEmpty()) {
                if (((byte[]) groups.getFirst()).length > buf.remaining()) {
                    break;
                }
                buf.put((byte[]) groups.removeFirst());
                ngroups++;
            }
            if (ngroups > 0) {
                buf.putInt(ngroupsPos, ngroups);
            }

            // store the host last so we support backward compatability
            buf.put(host);

            packets.add(new DatagramPacket(buf.array(),
                    buf.position(),
                    Constants.getRequestAddress(),
                    Constants.getDiscoveryPort()));
        } while (!groups.isEmpty());

        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, "encoded {0}", new Object[]{request});
        }
        return (DatagramPacket[])
                packets.toArray(new DatagramPacket[packets.size()]);
    }

    public MulticastRequest decodeMulticastRequest(
            DatagramPacket packet,
            InvocationConstraints constraints,
            ClientSubjectChecker checker)
            throws IOException {
        checkConstraints(constraints);
        if (checker != null) {
            checker.checkClientSubject(null);
        }

        try {
            ByteBuffer buf = ByteBuffer.wrap(packet.getData(),
                    packet.getOffset(),
                    packet.getLength());

            // read protocol version
            int version = buf.getInt();
            if (version != PROTOCOL_VERSION_1) {
                throw new DiscoveryProtocolException(
                        "wrong protocol version: " + version);
            }
            byte type = buf.get();
            if (type != REQUEST_TYPE) {
                throw new DiscoveryProtocolException(
                        "invalid type (we might get this when announcement and request are running on different address but using the same port (os bug)): " + type);
            }

            // derive client host
            String host = packet.getAddress().getHostAddress();
            // read client port
            int port = buf.getInt();
            System.out.println("-------------------------Discover1 host=" +host +", port=" + port );

            // read known service IDs
            int nids = buf.getInt();
            if (nids < 0 || nids > buf.remaining() / SERVICE_ID_LEN) {
                throw new DiscoveryProtocolException(
                        "invalid service ID count: " + nids);
            }
            ServiceID[] ids = new ServiceID[nids];
            for (int i = 0; i < ids.length; i++) {
                long hi = buf.getLong();
                long lo = buf.getLong();
                ids[i] = new ServiceID(hi, lo);
            }

            // read lookup groups
            int ngroups = buf.getInt();
            if (ngroups < 0 || ngroups > buf.remaining() / 2) {
                throw new DiscoveryProtocolException(
                        "invalid group count: " + ngroups);
            }
            String[] groups = new String[ngroups];
            for (int i = 0; i < groups.length; i++) {
                groups[i] = Plaintext.getUtf(buf);
            }

            try {
                host = Plaintext.getUtf(buf);
            } catch (Exception e) {
                // ignore, we got a request from an old version
            }

            MulticastRequest req =
                    new MulticastRequest(host, port, groups, ids);
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "decoded {0}", new Object[]{req});
            }
            return req;

        } catch (RuntimeException e) {
            throw new DiscoveryProtocolException(null, e);
        }
    }

    public EncodeIterator encodeMulticastAnnouncement(
            final MulticastAnnouncement announcement,
            final int maxPacketSize,
            final InvocationConstraints constraints) {
        if (maxPacketSize < MIN_MAX_PACKET_SIZE) {
            throw new IllegalArgumentException("maxPacketSize too small");
        }
        return new EncodeIterator() {

            private boolean used;

            public DatagramPacket[] next() throws IOException {
                used = true;
                return encode(announcement, maxPacketSize, constraints);
            }

            public boolean hasNext() {
                return !used;
            }
        };
    }

    private static DatagramPacket[] encode(MulticastAnnouncement announcement,
                                           int maxPacketSize,
                                           InvocationConstraints constraints)
            throws IOException {
        final int MIN_DATA_LEN = 28;

        checkConstraints(constraints);

        // precompute length of UTF-encoded group names
        LinkedList groups = new LinkedList();
        byte[] host = Plaintext.toUtf(announcement.getHost());
        String[] g = announcement.getGroups();
        for (int i = 0; i < g.length; i++) {
            byte[] b = Plaintext.toUtf(g[i]);
            if (b.length + host.length + MIN_DATA_LEN > maxPacketSize) {
                throw new DiscoveryProtocolException(
                        "group too long: " + g[i]);
            }
            groups.add(b);
        }

        List packets = new ArrayList();
        do {
            ByteBuffer buf = ByteBuffer.allocate(maxPacketSize);

            // write protocol version
            buf.putInt(PROTOCOL_VERSION_1);
            buf.put(ANNOUNCEMENT_TYPE);

            // write LUS host
            buf.put(host);

            // write LUS port
            buf.putInt(announcement.getPort());

            // write LUS service ID
            ServiceID id = announcement.getServiceID();
            buf.putLong(id.getMostSignificantBits());
            buf.putLong(id.getLeastSignificantBits());

            // write LUS member groups
            int ngroupsPos = buf.position();
            int ngroups = 0;
            buf.putInt(ngroups);
            while (!groups.isEmpty()) {
                if (((byte[]) groups.getFirst()).length > buf.remaining()) {
                    break;
                }
                buf.put((byte[]) groups.removeFirst());
                ngroups++;
            }
            if (ngroups > 0) {
                buf.putInt(ngroupsPos, ngroups);
            }

            packets.add(new DatagramPacket(buf.array(),
                    buf.position(),
                    Constants.getAnnouncementAddress(),
                    Constants.getDiscoveryPort()));
        } while (!groups.isEmpty());

        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, "encoded {0}",
                    new Object[]{announcement});
        }
        return (DatagramPacket[])
                packets.toArray(new DatagramPacket[packets.size()]);
    }

    public MulticastAnnouncement decodeMulticastAnnouncement(
            DatagramPacket packet,
            InvocationConstraints constraints)
            throws IOException {
        checkConstraints(constraints);

        try {
            ByteBuffer buf = ByteBuffer.wrap(packet.getData(),
                    packet.getOffset(),
                    packet.getLength());

            // read protocol version
            int version = buf.getInt();
            if (version != PROTOCOL_VERSION_1) {
                throw new DiscoveryProtocolException(
                        "wrong protocol version: " + version);
            }
            byte type = buf.get();
            if (type != ANNOUNCEMENT_TYPE) {
                throw new DiscoveryProtocolException(
                        "invalid type (we might get this when announcement and request are running on different address but using the same port (os bug)): " + type);
            }

            // read LUS host
            String host = Plaintext.getUtf(buf);

            // read LUS port
            int port = buf.getInt();

            // read LUS service ID
            long hi = buf.getLong();
            long lo = buf.getLong();
            ServiceID id = new ServiceID(hi, lo);

            // read LUS member groups
            int ngroups = buf.getInt();
            if (ngroups < 0 || ngroups > buf.remaining() / 2) {
                throw new DiscoveryProtocolException(
                        "invalid group count: " + ngroups);
            }
            String[] groups = new String[ngroups];
            for (int i = 0; i < groups.length; i++) {
                groups[i] = Plaintext.getUtf(buf);
            }

            MulticastAnnouncement ann =
                    new MulticastAnnouncement(-1, host, port, groups, id);
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "decoded {0}", new Object[]{ann});
            }
            return ann;

        } catch (RuntimeException e) {
            throw new DiscoveryProtocolException(null, e);
        }
    }

    public UnicastResponse doUnicastDiscovery(
            Socket socket,
            InvocationConstraints constraints,
            ClassLoader defaultLoader,
            ClassLoader verifierLoader,
            Collection context)
            throws IOException, ClassNotFoundException {
        checkConstraints(constraints);

        try {
            DataOutputStream dout = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream(), 4));

            // write unicast request
            dout.writeInt(PROTOCOL_VERSION_1);
            dout.flush();

            // derive LUS host, port
            String host = socket.getInetAddress().getHostAddress();
            int port = socket.getPort();

            // read LUS proxy
            ObjectInputStream oin = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            byte[] registrarBytes = new byte[oin.readInt()];
            oin.readFully(registrarBytes);
//	    MarshalledInstance mi =
//		new MarshalledInstance((MarshalledObject) oin.readObject());
//	    ServiceRegistrar reg =
//		(ServiceRegistrar) mi.get(defaultLoader, false, null, context);

            // read LUS member groups
            int ngroups = oin.readInt();
            if (ngroups < 0) {
                throw new DiscoveryProtocolException(
                        "invalid group count: " + ngroups);
            }
            String[] groups = new String[ngroups];
            for (int i = 0; i < groups.length; i++) {
                groups[i] = oin.readUTF();
            }

            socket.close();

            if (context == null) {
                context = Collections.EMPTY_SET;
            }
            ObjectInputStream os = new MarshalInputStream(new OptimizedByteArrayInputStream(registrarBytes), defaultLoader, false, null, context);
            ServiceRegistrar reg = (ServiceRegistrar) os.readObject();

            UnicastResponse resp =
                    new UnicastResponse(host, port, groups, reg);
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "received {0}", new Object[]{resp});
            }
            return resp;

        } catch (RuntimeException e) {
            throw new DiscoveryProtocolException(null, e);
        }
    }

    public void handleUnicastDiscovery(UnicastResponse response,
                                       Socket socket,
                                       InvocationConstraints constraints,
                                       ClientSubjectChecker checker,
                                       Collection context)
            throws IOException {
        checkConstraints(constraints);
        if (checker != null) {
            checker.checkClientSubject(null);
        }

        // note: unicast request (the protocol version) already consumed

        // write LUS proxy
        ObjectOutputStream oout = new ObjectOutputStream(
                new BufferedOutputStream(socket.getOutputStream()));
//    if (response.getMarshalledObjectRegistrar() != null) {
//        oout.writeObject(response.getMarshalledObjectRegistrar());
//    } else {
//        oout.writeObject(
//            new MarshalledInstance(
//            response.getRegistrar(), context).convertToMarshalledObject());
//    }
        oout.writeInt(response.getRegistrarBytes().length);
        oout.write(response.getRegistrarBytes());

        // write LUS member groups
        String[] groups = response.getGroups();
        oout.writeInt(groups.length);
        for (int i = 0; i < groups.length; i++) {
            oout.writeUTF(groups[i]);
        }

        oout.flush();
        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, "sent {0}", new Object[]{response});
        }
    }

    public String toString() {
        return "DiscoveryV1";
    }

    private static void checkConstraints(InvocationConstraints constraints)
            throws UnsupportedConstraintException {
        if (constraints != null) {
            constraints = constraints.makeAbsolute();
        }
        Plaintext.checkConstraints(constraints);
    }
}
