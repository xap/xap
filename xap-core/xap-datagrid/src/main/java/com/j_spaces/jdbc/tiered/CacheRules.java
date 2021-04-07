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
package com.j_spaces.jdbc.tiered;

import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class CacheRules {
    private Map<String, TimePredicate> rules;

    CacheRules() {
    }

    public static void validateTiered(QueryTemplatePacket template) throws IOException, SQLException {
        if (!TieredConfig.isCacheRulesEnabled()) {
            return;
        }
        if (template == null) {
            throw new SQLException("Doesnt match rule");
        }
        TimePredicate predicate = TieredConfig.getCacheRules().getRules().get(template.getTypeName());
        if (predicate != null) {
            if (!predicate.evaluate(template)) {
                throw new SQLException("Doesnt match rule");
            }
        }
    }

    public Map<String, TimePredicate> getRules() {
        return rules;
    }

    public CacheRules setRules(Map<String, TimePredicate> rules) {
        this.rules = rules;
        return this;
    }
}
