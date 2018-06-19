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

package org.openspaces.jpa.openjpa.query;

/**
 * Represents a node in the translated query expression tree.
 *
 * @author idan
 * @since 8.0
 */
public interface ExpressionNode {
    /**
     * Represents the node type in the translated query expression tree.
     */
    public enum NodeType {
        BINARY_EXPRESSION, LOGICAL_EXPRESSION, NULL_VALUE, PARAMETER, VARIABLE,
        FIELD_PATH, LITERAL_VALUE, EMPTY_EXPRESSION, INNER_QUERY, AGGREGATION_FUNCTION, CONTAINS_EXPRESSION, VARIABLE_BINDING, LIKE_EXPRESSION
    }

    /**
     * Appends the node SQL string to the string builder.
     *
     * @param sql The SQL string builder to append to.
     */
    void appendSql(StringBuilder sql);

    /**
     * Gets the node type
     *
     * @return The node type.
     */
    NodeType getNodeType();
}
