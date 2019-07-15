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

package com.gigaspaces.internal.io;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class FileUtils {
    public static File[] findFiles(File folder, final String prefix, final String suffix) {
        return folder.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (!file.isFile())
                    return false;
                final String fileName = file.getName();
                if (prefix != null && !fileName.startsWith(prefix))
                    return false;
                if (suffix != null && !fileName.endsWith(suffix))
                    return false;
                return true;
            }
        });
    }

    public static boolean deleteFileOrDirectoryIfExists(File fileOrDirectory) {
        if (fileOrDirectory.isDirectory()) {
            for (File file : BootIOUtils.listFiles(fileOrDirectory))
                deleteFileOrDirectoryIfExists(file);
        }
        return fileOrDirectory.delete();
    }

    public static void copyFile(File sourceFile, File destFile) throws IOException {
        try (FileInputStream fileInputStream = new FileInputStream(sourceFile);
             FileChannel sourceChannel = fileInputStream.getChannel();
             FileOutputStream fileOutputStream = new FileOutputStream(destFile);
             FileChannel destChannel = fileOutputStream.getChannel()) {

            destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
        }
    }
}
