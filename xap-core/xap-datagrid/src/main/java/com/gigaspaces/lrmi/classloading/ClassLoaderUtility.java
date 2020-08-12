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



