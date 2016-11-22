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

package org.openspaces.lucene.common;

import org.apache.lucene.analysis.Analyzer;

/**
 * @author Vitaliy_Zinchenko
 * @since 12.1
 */
final public class Utils {

    public Utils() {
    }

    public static Analyzer createAnalyzer(Class analyzerClass) {
        try {
            return (Analyzer) analyzerClass.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to instantiate analyzer " + analyzerClass, e);
        }
    }

    public static String makePath(String property, String relativePath) {
        return relativePath.length() == 0 ? property : property + "." + relativePath;
    }

}
