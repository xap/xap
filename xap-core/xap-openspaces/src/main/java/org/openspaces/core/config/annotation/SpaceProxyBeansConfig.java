/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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
package org.openspaces.core.config.annotation;

import org.openspaces.core.space.AbstractSpaceFactoryBean;
import org.openspaces.core.space.SpaceProxyFactoryBean;

/**
 * @author Niv Ingberg
 * @since 15.0
 */
public class SpaceProxyBeansConfig extends AbstractSpaceBeansConfig {
    @Override
    protected AbstractSpaceFactoryBean createSpaceFactoryBean() {
        SpaceProxyFactoryBean factoryBean = new SpaceProxyFactoryBean();
        configure(factoryBean);
        return factoryBean;
    }

    protected void configure(SpaceProxyFactoryBean factoryBean) {
        factoryBean.setSpaceName(getSpaceName());
    }
}
