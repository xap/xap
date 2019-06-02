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

package org.openspaces.pu.container.support;

import org.jini.rio.boot.ServiceClassLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A specific pattern resolver that handles the following:
 *
 * 1. Allows to create matching on "file system" resource which actually resides on webster
 *
 * Note: This is only applicable when not downloading the processing unit. Or when we are
 * downloading the processing unit, and shared-lib is used (which is not longer recommended in
 * 7.0).
 *
 * @author kimchy
 */
public class PUPathMatchingResourcePatternResolver extends PathMatchingResourcePatternResolver {

    protected Resource convertClassLoaderURL(URL url) {
        if (!(getClassLoader() instanceof ServiceClassLoader)) {
            return super.convertClassLoaderURL(url);
        }
        // add a special case when working with webster, we can list with http
        if (url.getProtocol().equals("http") && url.toExternalForm().endsWith("/")) {
            return new WebsterResoruce(url);
        }
        return new UrlResource(url);
    }

    protected Set doFindMatchingFileSystemResources(File rootDir, String subPattern) throws IOException {
        Set result = super.doFindMatchingFileSystemResources(rootDir, subPattern);
        Set actualResult = new LinkedHashSet();
        for (Object val : result) {
            if (!(val instanceof FileSystemResource)) {
                continue;
            }
            FileSystemResource fsResource = (FileSystemResource) val;
            if (fsResource.getFile() instanceof WebsterFile) {
                WebsterFile websterFile = (WebsterFile) fsResource.getFile();
                actualResult.add(new UrlResource(websterFile.toURL()));
            } else {
                actualResult.add(fsResource);
            }
        }
        return actualResult;
    }

    @Override
    protected Set<Resource> doFindPathMatchingJarResources(Resource rootDirResource, URL rootDirURL, String subPattern) throws IOException {
        try {
            return super.doFindPathMatchingJarResources(rootDirResource, rootDirURL, subPattern);
        } catch (IOException e) {
            // ignore exceptions on shraed-lib, since they come and go when we undeploy and deploy from the FS
            // but still remain in the CommonClassLoader
            if (rootDirResource.getURL().toExternalForm().indexOf("shared-lib") != -1) {
                return new LinkedHashSet();
            }
            throw e;
        }
    }

}
