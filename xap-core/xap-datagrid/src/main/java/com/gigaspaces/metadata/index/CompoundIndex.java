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

package com.gigaspaces.metadata.index;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryCompoundIndexGetter;
import com.gigaspaces.server.ServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * definition of a compound index meta data
 *
 * @author Yechiel
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class CompoundIndex extends CustomIndex {
    private static final long serialVersionUID = 1L;


    private ISpaceCompoundIndexSegment[] _segments;


    public CompoundIndex() {
        super();
    }

    public CompoundIndex(String[] paths) {
        super(SpaceIndexFactory.createCompoundIndexName(paths), new SpaceEntryCompoundIndexGetter((SpaceIndexFactory.createCompoundSegmentsDefinition(paths))), false /*isUnique*/, SpaceIndexType.EQUAL);
        _segments = ((SpaceEntryCompoundIndexGetter) _indexValueGetter).getSegments();
    }


    CompoundIndex(String indexName,
                  SpaceIndexType indexType, ISpaceCompoundIndexSegment[] segments, boolean unique) {
        super(indexName, new SpaceEntryCompoundIndexGetter(segments), unique, indexType);
        _segments = segments;
    }

    @Override
    public boolean isCompoundIndex() {
        return true;
    }

    @Override
    public ISpaceCompoundIndexSegment[] getCompoundIndexSegments() {
        return _segments;
    }

    @Override
    public int getNumSegments() {
        return _segments.length;
    }

    @Override
    public Object getIndexValueForTemplate(ServerEntry entry) {
        return _indexValueGetter.getValue(entry);
    }

    @Override
    public Object getIndexValue(ServerEntry entry) {
        return _indexValueGetter.getValue(entry);
    }

    /*
     * 2 compound indices are equivalent if they have the same segments 
     */
    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj))
            return false;
        CompoundIndex other = (CompoundIndex) obj;
        return Arrays.equals(_segments, other.getCompoundIndexSegments());
    }

    public boolean isEquivalent(CompoundIndex other) {
        return Arrays.equals(_segments, other.getCompoundIndexSegments());
    }


    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);
        _segments = IOUtils.readObject(in);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _segments = IOUtils.readObject(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);
        IOUtils.writeObject(out, _segments);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeObject(out, _segments);
    }
}
