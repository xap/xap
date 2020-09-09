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

package com.gigaspaces.metrics;

import com.gigaspaces.internal.utils.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class MetricPatternSet {
    private final String separator;
    private final Map<String, MetricPattern> patterns = new LinkedHashMap<>();

    public MetricPatternSet(String separator) {
        this.separator = separator;
    }

    public String getSeparator() {
        return separator;
    }

    public void add(String pattern, String sampler) {
        patterns.put(pattern, new MetricPattern(pattern, sampler, this));
    }

    public String findBestMatch(String s) {
        MetricPattern p1 = new MetricPattern(s, null, this);
        MetricPattern result = null;
        for (MetricPattern pattern : patterns.values()) {
            if (p1.match(pattern))
                result = bestMatch(result, pattern);
        }

        return result != null ? result.getValue() : "default";
    }

    String[] split(String pattern) {
        return StringUtils.tokenizeToStringArray(pattern, separator);
    }

    private static MetricPattern bestMatch(MetricPattern currPattern, MetricPattern newPattern) {
        if (currPattern == null)
            return newPattern;
        if (currPattern.getTokens() < newPattern.getTokens())
            return newPattern;
        if (currPattern.getJokers() > newPattern.getJokers())
            return newPattern;
        return currPattern;
    }

    public MetricPattern getPattern(String pattern) {
        return patterns.get(pattern);
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(", ");
        patterns.forEach((k, v) -> sj.add(k + " => " + v.getValue()));
        return sj.toString();
    }
}
