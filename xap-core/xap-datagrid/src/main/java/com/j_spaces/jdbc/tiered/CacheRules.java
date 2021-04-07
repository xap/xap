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
