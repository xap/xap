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
package com.gigaspaces.query.sql.functions;


import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Returns a substring of a given string
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */
public class SubstringSqlFunction extends SqlFunction {

    /**
     * @param context contains one String argument can furthermore contain the following options:
     *                -Two strings, the first an SQL regex, the other an escape character
     *                -One string, a posix regex
     *                -Two integers.
     * @return a substring according to the arguments given.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, 3, context);
        Object textObject = context.getArgument(0);
        if (!isString(textObject)) {
            throw new RuntimeException("Substring function - 1st argument must be a String");
        }
        String text = (String) textObject;
        Object fromExpression = context.getArgument(1);
        if (!isWholeNumber(fromExpression) && !isString(fromExpression))
            throw new RuntimeException("Substring function - 2nd argument must be an Intger or a String:" + fromExpression);
        Object forExpression = null;
        if (context.getNumberOfArguments() == 3) {
            forExpression = context.getArgument(2);
            if (!forExpression.getClass().equals(fromExpression.getClass()))
                throw new RuntimeException("Substring function - 3rd argument must be same type as 2nd argument:" + forExpression);
        }
        if (isString(fromExpression)) {
            String regFrom;
            if (forExpression == null) {
                // POSIX regex
                regFrom = ((String) fromExpression);
                boolean beginWithStar = regFrom.startsWith(".*");
                boolean endWithStar = regFrom.endsWith(".*");
                int matchingGroup = 0;
                if (!beginWithStar && !endWithStar) {
                    regFrom = "(.*)?(" + regFrom + ")(.*)?";
                    matchingGroup = 2;
                } else if (beginWithStar && !endWithStar) {
                    regFrom = "(" + regFrom + ")(.*)?";
                    matchingGroup = 1;
                } else if (!beginWithStar && endWithStar) {
                    regFrom = "(.*)?(" + regFrom + ")";
                    matchingGroup = 2;
                } else { //beginWithStar && endWithStar
                    matchingGroup = 1;
                }
                Pattern pattern = Pattern.compile(regFrom);
                Matcher matcher = pattern.matcher(text);
                return matcher.matches() ? matcher.group(matchingGroup) : "";
            } else {
                //SQL regex - for expression is the escape character
                regFrom = Like.sqlToRegexSimilar((String) fromExpression, (String) forExpression);
                String posixRegex = regFrom.replaceAll("\\\\\"", "");
                Pattern pattern = Pattern.compile(posixRegex);
                Matcher matcher = pattern.matcher(text);
                if (matcher.matches()) {
                    posixRegex = regFrom.substring(regFrom.indexOf("\\\"") + 2, regFrom.lastIndexOf("\\\""));
                    if (posixRegex.contains("\\\"")) {
                        throw new RuntimeException("SQL regular expression may not contain more than two escape-double-quote separators");
                    }
                    pattern = Pattern.compile("(.*)?(" + posixRegex + ")(.*)?");
                    matcher = pattern.matcher(text);
                    return matcher.matches() ? matcher.group(2) : "";
                } else {
                    return "";
                }
            }
        } else { //whole numbers
            int start = ((Number) fromExpression).intValue() - 1;
            if (forExpression != null){
                int count = ((Number)forExpression).intValue();
                if (count < 0) {
                    throw new RuntimeException("Substr function - negative substring length not allowed:" + count);
                }
                if (start < 0) {
                    if (start + count <= 0) {
                        return "";
                    }
                    return text.substring(0, start + count);
                }
                if (start + count > text.length()) {
                    return text.substring(start);
                }
                return text.substring(start, start + count);
            }
            if (start <= 0) {
                return text;
            }
            if (start > text.length()) {
                return "";
            }
            return text.substring(start);
        }
    }

}
