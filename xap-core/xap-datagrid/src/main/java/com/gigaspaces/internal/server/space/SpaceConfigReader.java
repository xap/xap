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

package com.gigaspaces.internal.server.space;

import com.j_spaces.kernel.log.JProperties;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class SpaceConfigReader {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ENGINE);

    private final String _fullSpaceName;

    public SpaceConfigReader(String fullSpaceName) {
        this._fullSpaceName = fullSpaceName;
    }

    public String getFullSpaceName() {
        return _fullSpaceName;
    }

    public String getSpaceProperty(String key, String defaultValue) {
        return JProperties.getSpaceProperty(_fullSpaceName, key, defaultValue, true);
    }

    public String getSpaceProperty(String key, String defaultValue, boolean convert) {
        return JProperties.getSpaceProperty(_fullSpaceName, key, defaultValue, convert);
    }

    public void setSpaceProperty(String key, String value) {
        JProperties.setSpaceProperty(_fullSpaceName, key, value);
    }

    public boolean containsSpaceProperty(String key) {
        return JProperties.containsSpaceProperty(_fullSpaceName, key);
    }

    public void assertSpacePropertyNotExists(String key, String depracationVersion, String deletionVersion) {
        if (_logger.isWarnEnabled()) {
            String value = getSpaceProperty(key, null);
            if (value != null)
                _logger.warn("The setting '" + key +
                        "' has been deprecated since " + depracationVersion +
                        " and is not supported since " + deletionVersion +
                        " - its value is ignored. ");
        }
    }

    public boolean getBooleanSpaceProperty(String key, String defaultValue) {
        String result = getSpaceProperty(key, defaultValue, true);
        return Boolean.parseBoolean(result);
    }

    public boolean getBooleanSpaceProperty(String key, String defaultValue, boolean convert) {
        String result = getSpaceProperty(key, defaultValue, convert);
        return Boolean.parseBoolean(result);
    }

    public int getIntSpaceProperty(String key, String defaultValue) {
        String result = getSpaceProperty(key, defaultValue, true);
        return Integer.parseInt(result);
    }

    public int getIntSpaceProperty(String key, String defaultValue, boolean convert) {
        String result = getSpaceProperty(key, defaultValue, convert);
        return Integer.parseInt(result);
    }

    public long getLongSpaceProperty(String key, String defaultValue) {
        String result = getSpaceProperty(key, defaultValue, true);
        return Long.parseLong(result);
    }

    public long getLongSpaceProperty(String key, String defaultValue, boolean convert) {
        String result = getSpaceProperty(key, defaultValue, convert);
        return Long.parseLong(result);
    }

    public float getFloatSpaceProperty(String key, String defaultValue) {
        String result = getSpaceProperty(key, defaultValue, true);
        return Float.parseFloat(result);
    }

    public float getFloatSpaceProperty(String key, String defaultValue, boolean convert) {
        String result = getSpaceProperty(key, defaultValue, convert);
        return Float.parseFloat(result);
    }

    public List<String> getListSpaceProperty(String key, String defaultValue, String separator) {
        String rawList = getSpaceProperty(key, defaultValue);
        if (rawList.equals(defaultValue))
            return null;

        List<String> list = new ArrayList<String>();
        StringTokenizer st = new StringTokenizer(rawList, separator);

        while (st.hasMoreElements()) {
            String item = st.nextToken();
            if (item == null)
                continue;
            item = item.trim();
            if (item.length() == 0)
                continue;

            list.add(item);
        }
        return list;
    }

    public Set<String> getSetSpaceProperty(String key, String defaultValue, String separator) {
        String rawList = getSpaceProperty(key, defaultValue);
        if (rawList.equals(defaultValue))
            return null;

        Set<String> set = new HashSet<String>();
        StringTokenizer st = new StringTokenizer(rawList, separator);

        while (st.hasMoreElements()) {
            String item = st.nextToken();
            if (item == null)
                continue;
            item = item.trim();
            if (item.length() == 0)
                continue;

            set.add(item);
        }
        return set;
    }

    @SuppressWarnings("unchecked")
    public <T> T getObjectSpaceProperty(String key) {
        return (T) JProperties.getObjectSpaceProperty(_fullSpaceName, key);
    }
}
