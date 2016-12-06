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

import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.query.extension.metadata.DefaultQueryExtensionPathInfo;

import org.apache.lucene.analysis.Analyzer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Vitaliy_Zinchenko
 * @since 12.1
 */
@com.gigaspaces.api.InternalApi
public class TextAnalyzerQueryExtensionPathInfo extends DefaultQueryExtensionPathInfo {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private Class<? extends Analyzer> analyzerClass;

    /**
     * Required for Externalizable
     */
    public TextAnalyzerQueryExtensionPathInfo() {

    }

    public TextAnalyzerQueryExtensionPathInfo(Class<? extends Analyzer> analyzerClass) {
        this.analyzerClass = analyzerClass;
    }

    public TextAnalyzerQueryExtensionPathInfo(Class<? extends Analyzer> analyzerClass, Class<? extends QueryExtensionProvider> providerClass) {
        super(providerClass);
        this.analyzerClass = analyzerClass;
    }

    public Class<? extends Analyzer> getAnalyzerClass() {
        return analyzerClass;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(analyzerClass);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        analyzerClass = (Class<? extends Analyzer>) in.readObject();
    }

    @Override
    public boolean isIndexed() {
        return false;
    }
}
