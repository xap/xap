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

package com.gigaspaces.internal.dump;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class InternalDumpResult implements Serializable {

    private static final long serialVersionUID = 1;

    private final String fullPath;

    private final String name;

    private final FileInfo[] files;

    private final InternalDumpProcessorFailedException[] failedProcessors;

    private final String heapReportJson;


    public InternalDumpResult(String name, String fullPath, FileInfo[] files, InternalDumpProcessorFailedException[] failedProcessors, String heapReportJson) {
        this.name = name;
        this.fullPath = fullPath;
        this.files = files;
        this.failedProcessors = failedProcessors;
        this.heapReportJson = heapReportJson;
    }

    public String getName() {
        return this.name;
    }

    public String getHeapReportJson() {
        return heapReportJson;
    }

    public String getFullPath() {
        return fullPath;
    }

    public FileInfo[] getFiles() {
        return files;
    }

    public InternalDumpProcessorFailedException[] getFailedProcessors() {
        return failedProcessors;
    }

    public long downloadSize() {
        long downloadSize = 0;
        for (FileInfo fileInfo : files) {
            downloadSize += fileInfo.size;
        }
        return downloadSize;
    }

    public void download(InternalDumpProvider dumpProvider, File target, InternalDumpDownloadListener listener) throws Exception {
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(target));
        download(dumpProvider, zos, listener);
        zos.close();
    }

    public void download(InternalDumpProvider dumpProvider, File targetDirectory, String fileName, InternalDumpDownloadListener listener) throws Exception {
        if (!fileName.endsWith(".zip")) {
            fileName = fileName + ".zip";
        }
        File zipFile = new File(targetDirectory, fileName);
        download(dumpProvider, zipFile, listener);
    }

    public void download(InternalDumpProvider dumpProvider, ZipOutputStream zos, InternalDumpDownloadListener listener) throws IOException {
        for (FileInfo fileInfo : files) {
            downloadFile(dumpProvider, fileInfo, zos, listener);
        }
    }

    public void downloadFile(InternalDumpProvider dumpProvider, FileInfo fileInfo, ZipOutputStream zos, InternalDumpDownloadListener listener) throws IOException {
        try {
            zos.putNextEntry(new ZipEntry(name + "/" + fileInfo.getName()));
        } catch (ZipException e) {
            if (e.getMessage() != null && e.getMessage().contains("duplicate entry")) {
                // simply ignore this entry
                return;
            } else {
                throw e;
            }
        }
        long counter = 0;
        int size = 1024 * 1024;
        while (true) {
            byte[] result = dumpProvider.dumpBytes(name + "/" + fileInfo.getName(), counter, size);
            if (result.length == 0) {
                break;
            }
            if (listener != null) {
                listener.onDownload(result.length, name, fileInfo.getName());
            }
            zos.write(result);
            if (result.length < size) {
                // thats it
                break;
            }
            counter += result.length;
        }
    }

    public static class FileInfo implements Serializable {

        private static final long serialVersionUID = 1;

        private final String name;
        private long size;

        public FileInfo(String name, long size) {
            this.name = name;
            this.size = size;
        }

        public String getName() {
            return name;
        }

        public long getSize() {
            return size;
        }
    }
}