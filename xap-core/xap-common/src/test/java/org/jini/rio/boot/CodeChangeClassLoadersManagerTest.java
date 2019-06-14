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
package org.jini.rio.boot;

import org.junit.Test;

import java.net.URL;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by tamirs
 * on 12/4/16.
 */
public class CodeChangeClassLoadersManagerTest {
    ClassLoader  defaultClassLoader = new CodeChangeClassLoader(new URL[]{}, getClass().getClassLoader());
    boolean supportCodeChange = true;
    int maxClassLoaders = 2;

    CodeChangeClassLoadersManager codeChangeClassLoadersManager =
            new CodeChangeClassLoadersManager(defaultClassLoader, supportCodeChange, maxClassLoaders);

    @Test
    public void getTaskClassLoader() throws Exception {

        // insert for the first time version 5
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer_5 = new SupportCodeChangeAnnotationContainer("5");
        ClassLoader taskClassLoader_5 = codeChangeClassLoadersManager.getCodeChangeClassLoader(supportCodeChangeAnnotationContainer_5);

        // insert for the first time version 6
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer_6 = new SupportCodeChangeAnnotationContainer("6");
        ClassLoader taskClassLoader_6 = codeChangeClassLoadersManager.getCodeChangeClassLoader(supportCodeChangeAnnotationContainer_6);

        // insert for the first time version 5
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer_7 = new SupportCodeChangeAnnotationContainer("7");
        ClassLoader taskClassLoader_7 = codeChangeClassLoadersManager.getCodeChangeClassLoader(supportCodeChangeAnnotationContainer_7);

        // assert default
        assertTrue(codeChangeClassLoadersManager.getDefaultClassLoader().equals(defaultClassLoader));

        // assert saved class loaders
        ConcurrentHashMap<String, CodeChangeClassLoader> versionToTaskClassLoaderMap = codeChangeClassLoadersManager.getVersionToClassLoadersMap();
        assertTrue(versionToTaskClassLoaderMap.size() == 2);

        // assert versions
        Set<String> keySet = versionToTaskClassLoaderMap.keySet();
        assertTrue(keySet.contains("6"));
        assertTrue(keySet.contains("7"));
        assertFalse(keySet.contains("5"));

        // assert saved class loaders
        Collection<CodeChangeClassLoader> values = versionToTaskClassLoaderMap.values();
        assertTrue(values.size() == 2);
        //noinspection SuspiciousMethodCalls
        assertTrue(values.contains(taskClassLoader_6));
        //noinspection SuspiciousMethodCalls
        assertTrue(values.contains(taskClassLoader_7));
        //noinspection SuspiciousMethodCalls
        assertFalse(values.contains(taskClassLoader_5));

        // assert max class loaders
        assertTrue(codeChangeClassLoadersManager.getMaxClassLoaders() == 2);

        // assert support code change
        assertTrue(codeChangeClassLoadersManager.isSupportCodeChange());

        // get saved class loader with version 6
        ClassLoader taskClassLoader_6_secondTime = codeChangeClassLoadersManager.getCodeChangeClassLoader(supportCodeChangeAnnotationContainer_6);
        assertTrue(taskClassLoader_6_secondTime.equals(taskClassLoader_6));

        // get saved class loader with version 7
        ClassLoader taskClassLoader_7_secondTime = codeChangeClassLoadersManager.getCodeChangeClassLoader(supportCodeChangeAnnotationContainer_7);
        assertTrue(taskClassLoader_7_secondTime.equals(taskClassLoader_7));

        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer_8 = new SupportCodeChangeAnnotationContainer("8");
        codeChangeClassLoadersManager.getCodeChangeClassLoader(supportCodeChangeAnnotationContainer_8);

        // assert insert of new version
        //noinspection SuspiciousMethodCalls
        assertFalse(codeChangeClassLoadersManager.getVersionToClassLoadersMap().contains(taskClassLoader_6));


    }

    @Test(expected = UnsupportedOperationException.class)
    public void getTaskClassLoaderDisabledSupportCodeChange() throws Exception {
        ClassLoader  defaultClassLoader = new CodeChangeClassLoader(new URL[]{}, getClass().getClassLoader());
        boolean supportCodeChange = false;
        int maxClassLoaders = 2;

        //noinspection ConstantConditions
        CodeChangeClassLoadersManager codeChangeClassLoadersManager = new CodeChangeClassLoadersManager(defaultClassLoader, supportCodeChange, maxClassLoaders);

        // insert for the first time version 5
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer_5 = new SupportCodeChangeAnnotationContainer("5");

        assertFalse(codeChangeClassLoadersManager.isSupportCodeChange());
        codeChangeClassLoadersManager.getCodeChangeClassLoader(supportCodeChangeAnnotationContainer_5);

    }

}