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

package com.gigaspaces.lrmi.nio.filters;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.nio.IChannelWriter;
import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.Writer;
import com.gigaspaces.start.XapModules;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class IOBlockFilterManager implements IOFilterManager {

    private final IOBlockFilterContainer filterContainer;
    private final IOFilterContext filterContext;

    public IOBlockFilterManager(Reader reader, IChannelWriter writer,
                                IOBlockFilter filter) {
        writer.setFilterManager(this);
        reader.setFilterManager(this);
        this.filterContainer = new IOBlockFilterContainer(reader, writer);
        this.filterContext = filterContainer.createContext(filter);
    }

    static private IOFilterFactory _filterFactory;

    static {
        try {
            _filterFactory = loadFilterFactoryFromSystemProperty();
        } catch (ClassNotFoundException e) {
            LoggerFactory.getLogger(Constants.LOGGER_LRMI_FILTERS).error(
                            "Error while creating LRMI filter factory, make sure the filter class is available in the classpath" +
                                    " (lib/ext for service grid components, or the same classpath of the classloader that loaded " +
                                    XapModules.DATA_GRID.getJarFileName() + " for remote clients): "
                                    + e,
                            e);
        } catch (Exception e) {
            LoggerFactory.getLogger(Constants.LOGGER_LRMI_FILTERS).error("Error while creating LRMI filter factory: " + e, e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.gigaspaces.lrmi.nio.filters.IOFilterManager#writeBytesNonBlocking
     * (com.gigaspaces.lrmi.nio.Writer.Context)
     */
    public void writeBytesNonBlocking(Writer.Context writeContext) throws IOException, IOFilterException {
        filterContainer.writeBytesNonBlocking(writeContext, filterContext);
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * com.gigaspaces.lrmi.nio.filters.IOFilterManager#handleNoneBlockingContent
     * (com.gigaspaces.lrmi.nio.Reader.Context, byte[])
     */
    public byte[] handleNoneBlockingContant(Reader.Context ctx, byte[] bytes)
            throws IOException, IOFilterException {
        return filterContainer.handleNoneBlockingContent(ctx, bytes, filterContext);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.gigaspaces.lrmi.nio.filters.IOFilterManager#handleBlockingContant
     * (byte[])
     */
    public byte[] handleBlockingContant(byte[] bytes, int slowConsumerTimeout)
            throws ClosedChannelException, IOException, IOFilterException {
        return filterContainer.handleBlockingContant(bytes, filterContext, slowConsumerTimeout);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.gigaspaces.lrmi.nio.filters.IOFilterManager#writeBytesBlocking(java
     * .nio.ByteBuffer)
     */
    public void writeBytesBlocking(ByteBuffer dataBuffer) throws IOException, IOFilterException {
        filterContainer.writeBytesBlocking(dataBuffer, filterContext);
    }

    public static IOFilterManager createFilter(Reader reader, Writer writer,
                                               boolean mode, SocketChannel channel) throws Exception {
        SocketAddress remoteAddress = null;
        try {
            remoteAddress = channel.socket().getRemoteSocketAddress();
        } catch (Exception ignored) {
        }

        if (getFilterFactory() != null) {
            IOFilter filter = createfilter(mode, getFilterFactory(), (InetSocketAddress) remoteAddress);
            if (filter == null)
                return null;

            if (filter instanceof IOBlockFilter) {
                IOBlockFilterManager blockFilterManager = new IOBlockFilterManager(
                        reader, writer, (IOBlockFilter) filter);
                return blockFilterManager;
            }
            if (filter instanceof IOStreamFilter) {
                IOStreamFilterManager streamFilterManager = new IOStreamFilterManager(
                        reader, writer, (IOStreamFilter) filter);
                return streamFilterManager;
            }
            if (filter instanceof IOInternalFilter) {
                IOInternalFilterManager internalFilterManager = new IOInternalFilterManager(
                        reader, writer, (IOInternalFilter) filter);
                return internalFilterManager;
            }
            throw new IllegalArgumentException("Filter should implements "
                    + IOStreamFilter.class.getName() + " or "
                    + IOBlockFilter.class.getName() +
                    (filter == null ? "" : ", actual: " + filter.getClass()));
        }
        return null;
    }

    private static IOFilter createfilter(boolean mode,
                                         IOFilterFactory filterFactory, InetSocketAddress remoteAddress) throws Exception {
        IOFilter filter;
        if (mode) {
            filter = filterFactory.createClientFilter(remoteAddress);
        } else {
            filter = filterFactory.createServerFilter(remoteAddress);
        }
        return filter;
    }

    private static IOFilterFactory loadFilterFactoryFromSystemProperty() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        String className = System.getProperty(SystemProperties.LRMI_NETWORK_FILTER_FACTORY);
        if (className == null) {
            return null;
        }
        Class<IOFilterFactory> c = (Class<IOFilterFactory>) IOBlockFilterManager.class
                .getClassLoader().loadClass(className);
        IOFilterFactory filterFactory = c.newInstance();
        LoggerFactory.getLogger(Constants.LOGGER_LRMI_FILTERS).info("Created LRMI filter factory: " + className);
        String addressMatcherFile = System.getProperty(SystemProperties.LRMI_NETWORK_FILTER_FACTORY_ADDRESS_MATCHERS_FILE);
        if (addressMatcherFile != null)
            filterFactory = new AddressMatcherFilterFactoryDelegator(filterFactory, addressMatcherFile);

        return filterFactory;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.gigaspaces.lrmi.nio.filters.IOFilterManager#beginHandshake()
     */
    public void beginHandshake() throws IOFilterException, IOException {
        filterContainer.beginHandshake(filterContext);

    }


    public static void setFilterFactory(IOFilterFactory _filterFactory) {
        IOBlockFilterManager._filterFactory = _filterFactory;
    }


    public static IOFilterFactory getFilterFactory() {
        return _filterFactory;
    }
}
