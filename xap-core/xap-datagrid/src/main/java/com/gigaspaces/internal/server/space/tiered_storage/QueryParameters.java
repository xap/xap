package com.gigaspaces.internal.server.space.tiered_storage;

import java.util.ArrayList;
import java.util.List;

public class QueryParameters {
        final private List<String> columns;
        final private List<Object> values;

        public QueryParameters() {
            columns = new ArrayList<>();
            values = new ArrayList<>();
        }
        
        public void addParameter(String column, Object value){
            columns.add(column);
            values.add(value);
        }

        public List<String> getColumns() {
            return columns;
        }

        public List<Object> getValues() {
            return values;
        }
    }