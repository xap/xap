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

package com.gigaspaces.internal.query.predicate.comparison;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.AbstractTypeIntrospector;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.range.FunctionCallDescription;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * a contains predicate cascading from a single item
 *
 * @author Yechiel Fefer
 * @since 9.6
 */

@com.gigaspaces.api.InternalApi
public class ContainsItemPredicate extends ContainsPredicate {
    private static final long serialVersionUID = -4499197241040332527L;
    private String fullPath;

    /**
     * Default constructor for Externalizable.
     */
    public ContainsItemPredicate() {
    }

    public ContainsItemPredicate(Object expectedValue, String relativePath, short templateMatchCode) {
        this(expectedValue, relativePath, null, templateMatchCode);
    }

    public ContainsItemPredicate(Object expectedValue, String relativePath, FunctionCallDescription functionCallDescription, short templateMatchCode) {
        super(expectedValue, functionCallDescription, relativePath, templateMatchCode);
    }

    public ContainsItemPredicate(Object expectedValue, String relativePath, FunctionCallDescription functionCallDescription, short templateMatchCode, String relation, String typeName,String fullPath) {
        super(expectedValue, functionCallDescription, relativePath, templateMatchCode, relation, typeName);
        this.fullPath = fullPath;
    }

    @Override
    public boolean execute(Object target) {
        // Initialize tokens & property info array.
        // Verify all fields are initialized because several threads can call the
        // initialize method and we want to verify that no thread will pass this check
        // before the predicate is initialized.
        if (getTokens() == null || getContainsIndexes() == null || getPropertyInfo() == null)
            initialize();
        if (!anyCollectionInRelativePath())
            return executeNoCollection(target);

        return target != null && performMatching(target, 0, 0);
    }

    private boolean anyCollectionInRelativePath() {
        return !(getContainsIndexes().length == 0);
    }

    private boolean executeNoCollection(Object target) {
        Object item = target;
        for (int i = 0; i < _tokens.length && item != null; i++)
            item = AbstractTypeIntrospector.getNestedValue(item, i, getTokens(), getPropertyInfo(), getFieldPath());

        return (item != null && executePredicate(item)) ? true : false;

    }

    public String getFullPath() {
        return getFieldPath();
    }

    @Override
    protected String getContainsPath(){
        return fullPath;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);
        if(_templateMatchCode == TemplateMatchCodes.RELATION) {
            fullPath = IOUtils.readString(in);
        }
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);
        if(_templateMatchCode == TemplateMatchCodes.RELATION) {
            IOUtils.writeString(out, fullPath);
        }
    }
}
