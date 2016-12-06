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

package org.openspaces.textsearch;

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Danylo_Hurin.
 */
public class LuceneTextSearchQueryExtensionManagerTest {

    private LuceneTextSearchQueryExtensionManager _manager;

    @Before
    public void setup() throws Exception {
        QueryExtensionRuntimeInfo info = new QueryExtensionRuntimeInfo() {
            @Override
            public String getSpaceInstanceName() {
                return "dummy";
            }

            @Override
            public String getSpaceInstanceWorkDirectory() {
                return null;
            }
        };
        LuceneTextSearchQueryExtensionProvider provider = new LuceneTextSearchQueryExtensionProvider();
        LuceneTextSearchConfiguration configuration = new LuceneTextSearchConfiguration(provider, info);
        _manager = new LuceneTextSearchQueryExtensionManager(provider, info, configuration);
    }

    @Test
    public void testAcceptNullGridValue() throws IllegalArgumentException {
        try {
            _manager.accept("", "", LuceneTextSearchQueryExtensionManager.SEARCH_OPERATION_NAME, null, "");
            Assert.fail("Should throw an exception here");
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void testAcceptNullQueryValue() throws IllegalArgumentException {
        try {
            _manager.accept("", "", LuceneTextSearchQueryExtensionManager.SEARCH_OPERATION_NAME, "", null);
            Assert.fail("Should throw an exception here");
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void testAcceptWrongOperation() throws IllegalArgumentException {
        try {
            _manager.accept("", "", "wrong_operation", "", "");
            Assert.fail("Should throw an exception here");
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void testCreateQueryNullOperand() throws IllegalArgumentException {
        try {
            _manager.createQuery("", "", "", null);
            Assert.fail("Should throw an exception here");
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void testCreateQueryWrongOperand() throws IllegalArgumentException {
        try {
            _manager.createQuery("", "", "wrong_operation", "");
            Assert.fail("Should throw an exception here");
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void testConvertFieldNotStringFiled() throws IllegalArgumentException {
        try {
            _manager.convertField("", new Exception());
            Assert.fail("Should throw an exception here");
        } catch (IllegalArgumentException e) {}
    }

}
