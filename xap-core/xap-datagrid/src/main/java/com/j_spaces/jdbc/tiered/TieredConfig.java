package com.j_spaces.jdbc.tiered;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Properties;

public class TieredConfig {
    private static final String CACHE_RULES_PROPERTIES_FILE = System.getProperty("com.gs.jdbc.fallback-driver.rules.properties");
    private static final String CACHE_RULES_TYPE_PROP_NAME_PREFIX = "typeName";
    private static final String CACHE_RULES_TIME_COLUMN_PROP_NAME_PREFIX = "timeColumn";
    private static final String CACHE_RULES_PERIOD_PROP_NAME_PREFIX = "period";
    private static CacheRules singletonRules;


    static boolean isCacheRulesEnabled() {
        return CACHE_RULES_PROPERTIES_FILE != null;
    }

    public static CacheRules getCacheRules() throws IOException {
        if (singletonRules == null) {
            singletonRules = createCacheRules();
        }
        return singletonRules;
    }

    private static CacheRules createCacheRules() throws IOException {
        if (CACHE_RULES_PROPERTIES_FILE == null) {
            return null;
        }
        FileInputStream in = null;
        try {

            //Init cache rules
            Properties cacheProperties = new Properties();
            in = new FileInputStream(CACHE_RULES_PROPERTIES_FILE);
            cacheProperties.load(in);

            HashMap<String, TimePredicate> rules = new HashMap<>();
            boolean finished = false;
            int ruleNum = 1;
            while (!finished) {
                String typeName = cacheProperties.getProperty(CACHE_RULES_TYPE_PROP_NAME_PREFIX + ruleNum);

                if (typeName == null) {
                    finished = true;
                } else {
                    String timeColumn = cacheProperties.getProperty(CACHE_RULES_TIME_COLUMN_PROP_NAME_PREFIX + ruleNum);
                    String period = cacheProperties.getProperty(CACHE_RULES_PERIOD_PROP_NAME_PREFIX + ruleNum);
                    if (timeColumn == null || period == null) {
                        throw new IllegalStateException("one or more of the required cache rule config properties is null { " +
                                CACHE_RULES_TIME_COLUMN_PROP_NAME_PREFIX + ruleNum + " = " + timeColumn + ", " +
                                CACHE_RULES_PERIOD_PROP_NAME_PREFIX + ruleNum + " = " + period + "}");
                    }
                    Duration duration = Duration.parse(period);
                    rules.put(typeName, new TimePredicate(typeName, timeColumn, duration, false));
                    ruleNum++;
                }
            }

            return new CacheRules().setRules(rules);


        } catch (IOException e) {
            throw new IllegalStateException("could not create properties file from " + CACHE_RULES_PROPERTIES_FILE, e);
        } finally {
            if (in != null)
                in.close();
        }
    }

}
