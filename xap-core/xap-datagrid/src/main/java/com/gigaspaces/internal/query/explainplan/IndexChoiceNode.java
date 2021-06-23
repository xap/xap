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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class IndexChoiceNode implements SmartExternalizable {
    private static final long serialVersionUID = 3571265564286482562L;
    private String name; // AND or OR
    private List<IndexInfo> options;
    private IndexInfo chosen;

    public IndexChoiceNode(String name) {
        this.name = name;
        options = new ArrayList<>();
    }

    public IndexChoiceNode(String name, List<IndexInfo> options, IndexInfo chosen) {
        this.name = name;
        this.options = options;
        this.chosen = chosen;
    }

    public IndexChoiceNode() {
        this.options = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<IndexInfo> getOptions() {
        return options;
    }

    public void addOption(IndexInfo option) {
        this.options.add(option);
    }

    public IndexInfo getChosen() {
        return chosen;
    }

    public void setChosen(IndexInfo chosen) {
        this.chosen = chosen;
    }

    public IndexInfo getOptionByName(String name){
        for (IndexInfo indexInfo : options) {
            if(indexInfo.getName().equals(name))
                return indexInfo;
        }
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

        IOUtils.writeString(objectOutput, this.name);
        writeList(objectOutput, this.options);
        objectOutput.writeObject(this.chosen);
    }

    private void writeList(ObjectOutput objectOutput, List<IndexInfo> options) throws IOException {
        int size = options.size();
        objectOutput.writeInt(size);
        for (IndexInfo option : options) {
            objectOutput.writeObject(option);
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.name = IOUtils.readString(objectInput);
        this.options = readList(objectInput);
        this.chosen = (IndexInfo) objectInput.readObject();
    }

    private List<IndexInfo> readList(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        List<IndexInfo> res = new ArrayList<>();
        int size = objectInput.readInt();
        for(int i=0; i<size; i++){
            res.add((IndexInfo) objectInput.readObject());
        }
        return res;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexChoiceNode that = (IndexChoiceNode) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (options != null ? !options.equals(that.options) : that.options != null) return false;
        return !(chosen != null ? !chosen.equals(that.chosen) : that.chosen != null);

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (options != null ? options.hashCode() : 0);
        result = 31 * result + (chosen != null ? chosen.hashCode() : 0);
        return result;
    }
}
