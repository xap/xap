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

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class MetricPattern {
    private final String pattern;
    private final String value;
    private final String[] tokens;
    private final int jokers;

    public MetricPattern(String pattern, String value, MetricPatternSet owner) {
        this.pattern = pattern;
        this.value = value;
        this.tokens = owner.split(pattern);
        this.jokers = countJokers(tokens);
    }

    private final int countJokers(String[] tokens) {
        int result = 0;
        for (String token : tokens)
            if (token.equals("*"))
                result++;
        return result;
    }

    public boolean match(MetricPattern other) {
        if (this.pattern.equals(other.pattern))
            return true;

        if (other.tokens.length > this.tokens.length)
            return false;

        int length = other.tokens.length;
        for (int i = 0; i < length; i++)
            if (!other.tokens[i].equals("*") && !other.tokens[i].equals(this.tokens[i]))
                return false;

        return true;
    }

    public int getTokens() {
        return tokens.length;
    }

    public int getJokers() {
        return jokers;
    }

    public String getValue() {
        return value;
    }

    public String getPattern() {
        return pattern;
    }
}
