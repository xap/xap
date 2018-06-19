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

package com.gigaspaces.internal.lrmi;

import java.net.InetSocketAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.quote;

/**
 * Provides means for generating connection url from its description and creating the descriptor
 * from the connection url in its string form.
 *
 * @author Dan Kilman
 * @since 9.6
 */
@com.gigaspaces.api.InternalApi
public class ConnectionUrlDescriptor {

    private final String _protocol;
    private final String _hostname;
    private final int _port;
    private final long _pid;
    private final long _objectId;
    private final long _objectClassLoaderId;
    private final long _lrmiRuntimeId;
    private final String _serviceDetails;

    public ConnectionUrlDescriptor(
            String protocol,
            String hostname,
            int port,
            long pid,
            long objectId,
            long objectClassLoaderId,
            long lrmiRuntimeId,
            String serviceDetails) {
        _protocol = protocol;
        _hostname = hostname;
        _port = port;
        _pid = pid;
        _objectId = objectId;
        _objectClassLoaderId = objectClassLoaderId;
        _lrmiRuntimeId = lrmiRuntimeId;
        _serviceDetails = serviceDetails;
    }

    public String getProtocol() {
        return _protocol;
    }

    public String getHostname() {
        return _hostname;
    }

    public int getPort() {
        return _port;
    }

    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(_hostname, _port);
    }

    public long getPid() {
        return _pid;
    }

    public long getObjectId() {
        return _objectId;
    }

    public long getObjectClassLoaderId() {
        return _objectClassLoaderId;
    }

    public long getLrmiRuntimeId() {
        return _lrmiRuntimeId;
    }

    public String getServiceDetails() {
        return _serviceDetails;
    }

    @Override
    public String toString() {
        return toUrl();
    }

    private static final String SEPARATOR_1 = "://";
    private static final String SEPARATOR_2 = ":";
    private static final String SEPARATOR_3 = "/pid[";
    private static final String SEPARATOR_4 = "]/";
    private static final String SEPARATOR_5 = "_";
    private static final String SEPARATOR_6 = "_";
    private static final String SEPARATOR_7 = "_details[";
    private static final String SEPARATOR_8 = "]";

    private static final String GROUP_ANY = "(.*)";
    private static final String GROUP_ANY_POSITIVE_NUMBER = "([1-9][0-9]*)";
    private static final String GROUP_ANY_NUMBER = "(-?[0-9]+)";
    private static final String PROTOCOL = GROUP_ANY;
    private static final String HOSTNAME = GROUP_ANY;
    private static final String PORT = GROUP_ANY_POSITIVE_NUMBER;
    private static final String PID = GROUP_ANY_POSITIVE_NUMBER;
    private static final String OJBECT_ID = GROUP_ANY_NUMBER;
    private static final String OBJECT_CLASSLOADER_ID = GROUP_ANY_NUMBER;
    private static final String LRMI_RUNTIME_ID = GROUP_ANY_NUMBER;
    private static final String SERVICE_DETAILS = GROUP_ANY;

    private static final String CONNECTION_URL_REGEX =
            PROTOCOL +
                    quote(SEPARATOR_1) +
                    HOSTNAME +
                    quote(SEPARATOR_2) +
                    PORT +
                    quote(SEPARATOR_3) +
                    PID +
                    quote(SEPARATOR_4) +
                    OJBECT_ID +
                    quote(SEPARATOR_5) +
                    OBJECT_CLASSLOADER_ID +
                    quote(SEPARATOR_6) +
                    LRMI_RUNTIME_ID +
                    "(?:" +
                    quote(SEPARATOR_7) +
                    SERVICE_DETAILS +
                    quote(SEPARATOR_8) +
                    ")?";

    private static final Pattern CONNECTION_URL_PATTERN = Pattern.compile(CONNECTION_URL_REGEX);

    /**
     * returns a URL of the following format: <protocol>://<hostname>:<port>/pid/<remote-object-id-string>_<stubId>
     * NIO://192.168.10.174:53727/pid[7284]/611763683694280_1_7316247838020130362_details[class
     * com.sun.jini.reggie.GigaRegistrar]
     *
     * For future additions to this url please refer to this class test cases to see possible
     * backwards compatability limitations.
     *
     * In general, it would be better if the closing delimiter of any additional infomration will
     * not be so general. e.g: ] ) } _ This limitation only applies if the additional information is
     * general (i.e. String). In the more specific case where the additional information may be a
     * number, date, etc... any thing that can be described accurately by a regular expression, this
     * closing delimiter limitation probably does not apply.
     */
    public String toUrl() {
        StringBuilder url = new StringBuilder()
                .append(_protocol)
                .append(SEPARATOR_1)
                .append(_hostname)
                .append(SEPARATOR_2)
                .append(_port)
                .append(SEPARATOR_3)
                .append(_pid)
                .append(SEPARATOR_4)
                .append(_objectId)
                .append(SEPARATOR_5)
                .append(_objectClassLoaderId)
                .append(SEPARATOR_6)
                .append(_lrmiRuntimeId)
                .append(SEPARATOR_7)
                .append(_serviceDetails)
                .append(SEPARATOR_8);
        return url.toString();
    }

    public static ConnectionUrlDescriptor fromUrl(String connectionUrl) {
        if (connectionUrl == null)
            throw new IllegalArgumentException("connectionUrl is null");

        Matcher matcher = CONNECTION_URL_PATTERN.matcher(connectionUrl);
        if (!matcher.find())
            throw new IllegalArgumentException("Malformed connectionUrl: " + connectionUrl);

        String protocol = matcher.group(1);
        String hostname = matcher.group(2);
        int port = Integer.parseInt(matcher.group(3));
        int pid = Integer.parseInt(matcher.group(4));
        long objectId = Long.parseLong(matcher.group(5));
        long objectClassLoaderId = Long.parseLong(matcher.group(6));
        long lrmiRuntimeId = Long.parseLong(matcher.group(7));
        String serviceDetails = matcher.group(8);

        return new ConnectionUrlDescriptor(protocol,
                hostname,
                port,
                pid,
                objectId,
                objectClassLoaderId,
                lrmiRuntimeId,
                serviceDetails);
    }

}
