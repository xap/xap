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
package com.gigaspaces.lrmi.classloading;

/**
 * Created by tamirs
 * on 11/9/16.
 */
public class ClassLoaderUtility {
    public static String getClassLoaderHierarchy(ClassLoader cl){
        return getClassLoaderHierarchyRec(cl, 1);
    }

    private static String getClassLoaderHierarchyRec(ClassLoader cl, int depth){
        if(cl == null){
            return "";
        }
        else {
            return "\n" + depth + ") " + cl.toString() + getClassLoaderHierarchyRec(cl.getParent(), depth + 1);
        }
    }
}



