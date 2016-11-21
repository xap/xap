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


package org.openspaces.core.space.mode;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean;

/**
 * Executes the refresh context mode operation using OpenSpaces sync remoting (allowing to use
 * broadcast mode).
 *
 * <p>The executor accepts as a parameter the url of the Space that the referesh will be executed
 * through.
 *
 * @author kimchy
 */
public class RefreshContextLoaderExecutor {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Must specify the space url");
        }

        String spaceUrl = args[0];
        UrlSpaceConfigurer urlSpaceConfigurer = new UrlSpaceConfigurer(spaceUrl);
        GigaSpace gigaSpace = new GigaSpaceConfigurer(urlSpaceConfigurer.space()).gigaSpace();

        ExecutorSpaceRemotingProxyFactoryBean remotingProxyFactoryBean = new ExecutorSpaceRemotingProxyFactoryBean();
        remotingProxyFactoryBean.setGigaSpace(gigaSpace);
        remotingProxyFactoryBean.setServiceInterface(RefreshableContextLoader.class);
        remotingProxyFactoryBean.setBroadcast(true);
        remotingProxyFactoryBean.afterPropertiesSet();

        RefreshableContextLoader refreshableContextLoader = (RefreshableContextLoader) remotingProxyFactoryBean.getObject();
        refreshableContextLoader.refresh();

        urlSpaceConfigurer.close();
    }
}
