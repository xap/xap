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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class UnionIndexInfo extends IndexInfo {
    private static final long serialVersionUID = -4526394066353477719L;
    private List<IndexInfo> options;

    public UnionIndexInfo() {
    }

    public UnionIndexInfo(List<IndexInfo> options) {
        this.options = options;
        initialize(options);
    }

    public List<IndexInfo> getOptions() {
        return options;
    }

    private void initialize(List<IndexInfo> options) {
        StringBuilder name = new StringBuilder("[");
        Integer size = 0;
        boolean usable = (options.size() == 0);
        if (options.size() > 0) {
            for (IndexInfo option : options) {
                if (option.isUsable()) {
                    usable = true;
                }
                String optionName = "(" + option.getName() + " " + option.getOperator() + " " + option.getValue() + ")";
                name.append(optionName).append(", ");
                if (option.getSize() < 0){
                    size = -1;
                    break;
                }else{
                    size += option.getSize();
                }
            }
            name.deleteCharAt(name.length() - 1);
            name.deleteCharAt(name.length() - 1);
        }
        name.append("]");
        setName(name.toString());
        setSize(size);
        setUsable(usable);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeList(out, options);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.options = IOUtils.readList(in);
        initialize(options);
    }

    @Override
    public String toString() {
        return "Union [size=" + getSizeDesc() +
               (getSize() > 0 ? ", " + getName() : "") +
               "]";
    }
}
