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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * @author eitany
 * @since 9.5.1
 */
public class
AddressMatcherFilterFactoryDelegator
        implements IOFilterFactory {

    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_FILTERS);

    private final IOFilterFactory _filterFactory;
    private final List<Pattern> _addresMatchers;

    public AddressMatcherFilterFactoryDelegator(IOFilterFactory filterFactory, String addressMatchersFile) {
        _filterFactory = filterFactory;
        _addresMatchers = new LinkedList<Pattern>();

        final InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(addressMatchersFile);
        if (resourceAsStream == null) {
            throw new IllegalArgumentException("Could not locate lrmi filter factory address matchers file " + addressMatchersFile + " in the classpath");
        }
        if (_logger.isDebugEnabled())
            _logger.debug("Loading address matcher configuration file from " + addressMatchersFile);

        BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream));
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                //Rermarked line
                if (line.startsWith("#"))
                    continue;

                _addresMatchers.add(Pattern.compile(line));
            }
        } catch (PatternSyntaxException e) {
            throw new IllegalStateException("Error while parsing lrmi filter factory address matchers file " + addressMatchersFile, e);
        } catch (IOException e) {
            throw new IllegalStateException("Error while parsing lrmi filter factory address matchers file " + addressMatchersFile, e);
        }

    }

    @Override
    public IOFilter createClientFilter(InetSocketAddress remoteAddress)
            throws Exception {
        for (Pattern addressMatcher : _addresMatchers) {
            //Cause resolving
            remoteAddress.getHostName();
            if (addressMatcher.matcher(remoteAddress.toString()).matches()) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Created lrmi filter for a connection to " + remoteAddress);
                return _filterFactory.createClientFilter(remoteAddress);
            }
        }
        if (_logger.isDebugEnabled())
            _logger.debug("No address matcher found for connection to " + remoteAddress + ", no filter created");
        return null;
    }

    @Override
    public IOFilter createServerFilter(InetSocketAddress remoteAddress)
            throws Exception {
        for (Pattern addressMatcher : _addresMatchers) {
            //Cause resolving
            remoteAddress.getHostName();
            if (addressMatcher.matcher(remoteAddress.toString()).matches()) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Created lrmi filter for a connection to " + remoteAddress);
                return _filterFactory.createServerFilter(remoteAddress);
            }
        }
        if (_logger.isDebugEnabled())
            _logger.debug("No address matcher found for connection to " + remoteAddress + ", no filter created");
        return null;
    }

}
