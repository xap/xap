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

package org.openspaces.core.space;

import com.gigaspaces.server.eviction.SpaceEvictionStrategy;
import com.j_spaces.core.Constants;

import java.util.Properties;

/**
 * Configures the Space to run in Custom mode. Defaults value for all configuration will be based on
 * the schema chosen.
 *
 * @author Sagi Bernstein
 * @since 9.1.0
 */
public class CustomCachePolicy extends LruCachePolicy {

    private SpaceEvictionStrategy evictionStrategy;


    /**
     * Sets the custom eviction strategy to be used by the space, When using a Custom Cache Policy
     * this property must be set.
     */
    public CustomCachePolicy evictionStrategy(SpaceEvictionStrategy evictionStrategy) {
        setEvictionStrategy(evictionStrategy);
        return this;
    }

    /**
     * Sets the custom eviction strategy to be used by the space, When using a Custom Cache Policy
     * this property must be set.
     */
    public CustomCachePolicy setEvictionStrategy(SpaceEvictionStrategy evictionStrategy) {
        this.evictionStrategy = evictionStrategy;
        return this;
    }

    @Override
    public Properties toProps() {
        Properties props = super.toProps();
        props.setProperty(Constants.CacheManager.FULL_CACHE_POLICY_PROP, "" + Constants.CacheManager.CACHE_POLICY_PLUGGED_EVICTION);
        if (evictionStrategy != null)
            props.put(Constants.CacheManager.CACHE_MANAGER_EVICTION_STRATEGY_PROP, evictionStrategy);

        return props;
    }

}
