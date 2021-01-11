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

package com.j_spaces.jdbc.parser;

import com.gigaspaces.internal.io.IOUtils;
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;

/**
 * Created by Barak Bar Orion 8/24/15.
 */
@com.gigaspaces.api.InternalApi
public class RelationNode extends ExpNode {
    private static final long serialVersionUID = 1L;

    private String relation;

    public RelationNode() {
    }

    public RelationNode(ExpNode left, String relation, ExpNode right) {
        super(left, right);
        this.relation = relation;
    }

    @Override
    public boolean isValidCompare(Object ob1, Object ob2) {
        // Comparison with null is not supported
        //noinspection unchecked
        return !(ob1 == null || ob2 == null) && ((Comparable) ob1).compareTo(ob2) == 0;
    }

    @Override
    public ExpNode newInstance() {
        return new RelationNode();
    }

    @Override
    public void accept(QueryTemplateBuilder builder) throws SQLException {
        builder.build(this);
    }

    @Override
    public void setTemplate(QueryTemplatePacket template) {
        super.setTemplate(template);
    }

    public String getRelation() {
        return relation;
    }

    @Override
    public Object clone() {
        RelationNode cloned = (RelationNode) super.clone();
        cloned.relation = this.relation;
        return cloned;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(out, relation);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.relation = IOUtils.readString(in);
    }
}
