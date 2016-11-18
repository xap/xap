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

package org.openspaces.spatial.lucene.common;

import com.gigaspaces.query.extension.QueryExtensionProvider;

import java.util.Properties;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
public abstract class BaseLuceneQueryExtensionProvider extends QueryExtensionProvider {

    protected final Properties _customProperties;

    public BaseLuceneQueryExtensionProvider(Properties customProperties) {
        this._customProperties = customProperties;
    }

    public String getCustomProperty(String key, String defaultValue) {
        return _customProperties.getProperty(key, defaultValue);
    }

}
