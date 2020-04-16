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

package org.jini.rio.boot;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tamirs
 * on 11/8/16.
 */
@com.gigaspaces.api.InternalApi
public class CodeChangeClassLoadersManager {

    final private static Logger logger = LoggerFactory.getLogger("com.gigaspaces.lrmi.classloading");
    private static CodeChangeClassLoadersManager codeChangeClassLoadersManagerOfSpaceInstance;

    private final ClassLoader defaultClassLoader;
    private final boolean supportCodeChange;
    private final int maxClassLoaders;
    private final ConcurrentHashMap<String, CodeChangeClassLoader> versionToClassLoadersMap;

    public CodeChangeClassLoadersManager(ClassLoader defaultClassLoader, boolean supportCodeChange, int maxClassLoaders) {
        this.defaultClassLoader = defaultClassLoader;
        this.supportCodeChange = supportCodeChange;
        this.maxClassLoaders = maxClassLoaders;
        versionToClassLoadersMap = new ConcurrentHashMap<String, CodeChangeClassLoader>();
    }

    public ClassLoader getCodeChangeClassLoader(SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer) {
        if(!supportCodeChange){ // disable reloading
            throw new UnsupportedOperationException("Task has supportCodeAnnotation but it is disabled by space");
        }
        if(logger.isTraceEnabled()){
            logger.trace("Search for class-loader with version ["+supportCodeChangeAnnotationContainer.getVersion()+"] ");
        }
        if(supportCodeChangeAnnotationContainer.getVersion().isEmpty()){ // one time
            CodeChangeClassLoader codeChangeClassLoader = new CodeChangeClassLoader(new URL[]{}, defaultClassLoader);
            if(logger.isTraceEnabled()){
                logger.trace("Created new class-loader for one time use. New class loader is " + codeChangeClassLoader);
            }
            return codeChangeClassLoader;
        }
        else { // might be already loaded
            String classVersion = supportCodeChangeAnnotationContainer.getVersion();
            ClassLoader cachedClassLoader = versionToClassLoadersMap.get(classVersion);
            if(cachedClassLoader != null){ // cached class loader
                if(logger.isTraceEnabled()){
                    logger.trace("Found class-loader: " + cachedClassLoader + ", with version: " + classVersion);
                }
                return cachedClassLoader;
            }
            synchronized (this){
                // if 2 threads try to insert to map, while map was full,
                // both threads had same version on task -
                // then first one got the lock update the map, and the second one only need to do get on map
                CodeChangeClassLoader tcl = versionToClassLoadersMap.get(classVersion);
                if(tcl != null){
                    return tcl;
                }
                return insertToMap(classVersion);
            }
        }
    }

    private CodeChangeClassLoader insertToMap(String version) {
        if(versionToClassLoadersMap.size() == this.maxClassLoaders){
            removeOldestClassLoader();
        }
        CodeChangeClassLoader codeChangeClassLoader = new CodeChangeClassLoader(new URL[]{}, defaultClassLoader);
        versionToClassLoadersMap.put(version, codeChangeClassLoader);
        if(logger.isInfoEnabled()){
            logger.info("Added new class-loader ["+ codeChangeClassLoader +"] for version ["+version+"]");
        }
        if(logger.isTraceEnabled()){
            logger.trace("Class-loaders: " + versionToClassLoadersMap + " , max-class-loaders=["+maxClassLoaders+"]");
        }
        return codeChangeClassLoader;
    }

    private void removeOldestClassLoader() {
        String oldestVersion = findOldestVersion();
        CodeChangeClassLoader removedCodeChangeClassLoader = versionToClassLoadersMap.remove(oldestVersion);
        if(logger.isInfoEnabled()){
            logger.info("Limit of max-class-loaders=["+maxClassLoaders+"] reached, removing oldest class-loader ["+ removedCodeChangeClassLoader +"] for version ["+oldestVersion+"]");
        }
    }

    private String findOldestVersion() {
        long min = System.currentTimeMillis();
        String oldestVersion = null;
        for (Map.Entry<String, CodeChangeClassLoader> versionClassLoaderEntry : versionToClassLoadersMap.entrySet()) {
            String version = versionClassLoaderEntry.getKey();
            CodeChangeClassLoader codeChangeClassLoader = versionClassLoaderEntry.getValue();
            long classLoaderLoadTime = codeChangeClassLoader.getLoadTime();
            if(classLoaderLoadTime < min){
                min = classLoaderLoadTime;
                oldestVersion = version;
            }
        }
        return oldestVersion;
    }

    public SpaceInstanceRemoteClassLoaderInfo createSpaceInstanceRemoteClassLoaderInfo(){
        return new SpaceInstanceRemoteClassLoaderInfo(
                new RemoteClassLoaderInfo(defaultClassLoader.toString()),
                createClassLoaderInfoMap(versionToClassLoadersMap),
                this.maxClassLoaders);
    }

    private HashMap<String, RemoteClassLoaderInfo> createClassLoaderInfoMap(ConcurrentHashMap<String, CodeChangeClassLoader> versionToClassLoaderMap) {
        HashMap<String, RemoteClassLoaderInfo> map = new HashMap<String, RemoteClassLoaderInfo>();
        for (Map.Entry<String, CodeChangeClassLoader> versionClassLoaderEntry : versionToClassLoaderMap.entrySet()) {
            String version = versionClassLoaderEntry.getKey();
            CodeChangeClassLoader codeChangeClassLoader = versionClassLoaderEntry.getValue();
            map.put(version, codeChangeClassLoader.createRemoteClassLoaderInfo());
        }
        return map;
    }

    public static void initInstance(ClassLoader defaultClassLoader, boolean supportCodeChange, int maxClassLoaders) {
        codeChangeClassLoadersManagerOfSpaceInstance = new CodeChangeClassLoadersManager(defaultClassLoader, supportCodeChange, maxClassLoaders);
    }

    public static CodeChangeClassLoadersManager getInstance(){
        return codeChangeClassLoadersManagerOfSpaceInstance;
    }

    public ClassLoader getDefaultClassLoader() {
        return defaultClassLoader;
    }

    public int getMaxClassLoaders() {
        return maxClassLoaders;
    }

    public ConcurrentHashMap<String, CodeChangeClassLoader> getVersionToClassLoadersMap() {
        return versionToClassLoadersMap;
    }

    public boolean isSupportCodeChange() {
        return supportCodeChange;
    }
}
