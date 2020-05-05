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

package com.gigaspaces.persistency.datasource;

import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.persistency.Constants;
import com.gigaspaces.persistency.metadata.DefaultSpaceDocumentMapper;
import com.gigaspaces.persistency.metadata.SpaceDocumentMapper;
import com.gigaspaces.persistency.parser.SQL2MongoBaseVisitor;
import com.gigaspaces.persistency.parser.SQL2MongoLexer;
import com.gigaspaces.persistency.parser.SQL2MongoParser;
import com.gigaspaces.persistency.parser.SQL2MongoParser.ParseContext;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * @author Shadi Massalha
 */
public class MongoQueryFactory {

    private static final String LIKE = "like()";
    private static final String RLIKE = "rlike()";
    private static final String PARAM_PLACEHOLDER = "'%{}'";
    private static final Map<SpaceTypeDescriptor, Map<String, String>> cachedQuery = new ConcurrentHashMap<SpaceTypeDescriptor, Map<String, String>>();

    public static BasicDBObjectBuilder create(DataSourceQuery sql) {
        Map<String, String> cache = cachedQuery.get(sql.getTypeDescriptor());

        if (cache == null) {
            cache = new HashMap<String, String>();

            cachedQuery.put(sql.getTypeDescriptor(), cache);
        }

        String query = sql.getAsSQLQuery().getQuery();

        String parsedQuery = cache.get(query);

        if (parsedQuery == null) {
            parsedQuery = parse(query);

            cache.put(query, parsedQuery);
        }
        BasicDBObjectBuilder queryResult = bind(parsedQuery, sql
                .getAsSQLQuery().getQueryParameters(), sql.getTypeDescriptor());

        replaceIdProperty(queryResult, sql.getTypeDescriptor());

        return queryResult;
    }

    @SuppressWarnings("static-access")
    private static void replaceIdProperty(BasicDBObjectBuilder qResult,
                                          SpaceTypeDescriptor typeDescriptor) {

        DBObject q = qResult.get();

        if (q.containsField(typeDescriptor.getIdPropertyName())) {

            Object value = q.get(typeDescriptor.getIdPropertyName());

            q.removeField(typeDescriptor.getIdPropertyName());

            q.put(Constants.ID_PROPERTY, value);

            qResult.start(q.toMap());
        }
    }

    private static String parse(String sql) {
        ANTLRInputStream charstream = new ANTLRInputStream(sql);

        SQL2MongoLexer lexer = new SQL2MongoLexer(charstream);

        TokenStream tokenStream = new CommonTokenStream(lexer);

        SQL2MongoParser parser = new SQL2MongoParser(tokenStream);

        SQL2MongoBaseVisitor<ParseContext> visitor = new SQL2MongoBaseVisitor<ParseContext>();

        parser.parse().accept(visitor);

        return visitor.getQuery().toString();
    }

    public static BasicDBObjectBuilder bind(String parsedQuery,
                                            Object[] parameters, SpaceTypeDescriptor spaceTypeDescriptor) {

        SpaceDocumentMapper<DBObject> mapper = new DefaultSpaceDocumentMapper(
                spaceTypeDescriptor, false);

        DBObject obj = (DBObject) JSON.parse(parsedQuery);

        BasicDBObjectBuilder query = BasicDBObjectBuilder.start(obj.toMap());

        if (parameters != null) {
            query = replaceParameters(parameters, mapper, query, 0);
        }
        return query;
    }

    private static BasicDBObjectBuilder replaceParameters(Object[] parameters,
                                                          SpaceDocumentMapper<DBObject> mapper, BasicDBObjectBuilder builder,
                                                          Integer index) {

        BasicDBObjectBuilder newBuilder = BasicDBObjectBuilder.start();

        DBObject document = builder.get();

        Iterator<String> iterator = document.keySet().iterator();

        while (iterator.hasNext()) {
            String field = iterator.next();
            Object ph = document.get(field);

            if (index >= parameters.length)
                return builder;

            if (ph instanceof String) {
                if (PARAM_PLACEHOLDER.equals(ph)) {
                    Object p = mapper.toObject(parameters[index++]);

                    if (p instanceof DBObject
                            && Constants.CUSTOM_BINARY.equals(((DBObject) p)
                            .get(Constants.TYPE))) {
                        newBuilder.add(field + "." + Constants.HASH,
                                ((DBObject) p).get(Constants.HASH));
                    } else {
                        newBuilder.add(field, p);
                    }
                }
            } else if (ph instanceof Pattern) {
                Pattern p = (Pattern) ph;

                if (LIKE.equalsIgnoreCase(p.pattern())) {
                    newBuilder
                            .add(field,
                                    convertLikeExpression((String) parameters[index++]));
                } else if (RLIKE.equalsIgnoreCase(p.pattern())) {
                    newBuilder.add(field, Pattern.compile(
                            (String) parameters[index++],
                            Pattern.CASE_INSENSITIVE));
                }
            } else {
                DBObject element = (DBObject) ph;

                Object p = mapper.toObject(parameters[index]);

                if (p instanceof DBObject) {
                    String t = (String) ((DBObject) p).get(Constants.TYPE);
                    String op = element.keySet().iterator().next();

                    if (Constants.CUSTOM_BINARY.equals(t) && !op.equals("$ne"))
                        return newBuilder;
                }

                BasicDBObjectBuilder doc = replaceParameters(parameters,
                        mapper, BasicDBObjectBuilder.start(element.toMap()),
                        index);

                newBuilder.add(field, doc.get());
            }
        }
        return newBuilder;
    }

    private static Pattern convertLikeExpression(String val) {
        if (val != null && !val.isEmpty()) {
            if (val.charAt(0) == '\'')
                val = val.substring(1);

            if (val.charAt(val.length() - 1) == '\'')
                val = val.substring(0, val.length() - 1);

            if (val.charAt(0) != '%')
                val = "^" + val;
            else
                val = val.substring(1);

            if (val.charAt(val.length() - 1) != '%')
                val = val + "$";
            else
                val = val.substring(0, val.length() - 1);

            val = val.replaceAll("%", ".*");
            val = val.replace('_', '.');

            return Pattern.compile(val);
        }

        return Pattern.compile("");
    }

}
