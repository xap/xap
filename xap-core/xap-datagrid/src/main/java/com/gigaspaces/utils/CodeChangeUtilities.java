package com.gigaspaces.utils;

import com.gigaspaces.annotation.SupportCodeChange;
import com.gigaspaces.internal.classloader.ClassLoaderCache;
import org.jini.rio.boot.SupportCodeChangeAnnotationContainer;

import java.util.Collection;

/**
 * Created by tamirs
 * on 12/8/16.
 */
public class CodeChangeUtilities {

    public static void removeOneTimeClassLoaderIfNeeded(Collection objects) {
        for (Object object : objects) {
            ClassLoader oneTimeClassLoader = getOneTimeClassLoader(object);
            if(oneTimeClassLoader != null){
                ClassLoaderCache.getCache().removeClassLoader(oneTimeClassLoader);
            }
        }
    }

    private static ClassLoader getOneTimeClassLoader(Object object) {
        if(object.getClass().isAnnotationPresent(SupportCodeChange.class)){
            SupportCodeChange annotation = object.getClass().getAnnotation(SupportCodeChange.class);
            if(annotation.id().isEmpty()){
                return object.getClass().getClassLoader();
            }
        }
        return null;
    }

    public static SupportCodeChangeAnnotationContainer createContainerFromSupportCodeAnnotationIfNeeded(Object object) {
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer = null;
        Class<?> objectClass = object.getClass();
        if(objectClass.isAnnotationPresent(SupportCodeChange.class)) {
            SupportCodeChange annotation = objectClass.getAnnotation(SupportCodeChange.class);
            if (annotation.id().isEmpty()) {
                supportCodeChangeAnnotationContainer = SupportCodeChangeAnnotationContainer.ONE_TIME;
            } else {
                supportCodeChangeAnnotationContainer = new SupportCodeChangeAnnotationContainer(annotation.id());
            }
        }
        return supportCodeChangeAnnotationContainer;
    }
}
