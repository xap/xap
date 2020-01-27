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

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author evgeny
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class BlueprintZipUtils {

    final private static Logger logger = Logger.getLogger("com.gigaspaces.zip");

    public static void zip(File dir2zip, File zipFile) throws IOException {
        ZipArchiveOutputStream zaos = new ZipArchiveOutputStream(new FileOutputStream(zipFile));
        internalZipDir(dir2zip.getAbsolutePath(), dir2zip.getAbsolutePath(), zaos);
        IOUtils.closeQuietly( zaos );
    }

    private static void internalZipDir(String baseDir2zip, String dir2zip, ZipArchiveOutputStream zaos) throws IOException {
        //create a new File object based on the directory we have to zip
        File zipDir = new File(dir2zip);
        if (!zipDir.exists()) {
            throw new IllegalArgumentException("Directory to zip [" + zipDir.getAbsolutePath() + "] does not exists");
        }
        if (!zipDir.isDirectory()) {
            throw new IllegalArgumentException("Directory to zip [" + zipDir.getAbsolutePath() + "] is not a directory");
        }
        //get a listing of the directory content
        String[] dirList = zipDir.list();
        if (dirList == null) {
            return;
        }
/*
        byte[] readBuffer = new byte[2156];
        int bytesIn = 0;
*/
        //loop through dirList, and zip the files
        for (int i = 0; i < dirList.length; i++) {
            File f = new File(zipDir, dirList[i]);
            if (f.isDirectory()) {
                //if the File object is a directory, call this
                //function again to add its content recursively
                String filePath = f.getAbsolutePath();
                internalZipDir(baseDir2zip, filePath, zaos);
                //loop again
                continue;
            }
            //if we reached here, the File object f was not a directory
            //create a FileInputStream on top of f
            try (FileInputStream fis = new FileInputStream(f)) {
                // create a new zip entry
                String path = f.getAbsolutePath().substring(baseDir2zip.length() + 1);
                path = path.replace('\\', '/');
                ZipArchiveEntry anEntry = new ZipArchiveEntry( path );
                anEntry.setUnixMode( getUnixMode( f ) );
                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest("Compressing zip entry [" + anEntry.getName() + "] with size [" + anEntry.getSize() + "] at [" + f.getAbsolutePath() + "]");
                }
                //place the zip entry in the ZipOutputStream object
                zaos.putArchiveEntry(anEntry);
                IOUtils.copy( fis ,zaos );
                zaos.closeArchiveEntry();
/*
                //now write the content of the file to the ZipOutputStream
                while ((bytesIn = fis.read(readBuffer)) != -1) {
                    zaos.write(readBuffer, 0, bytesIn);
                }
*/
            }
        }
    }

    private static int getUnixMode( File file ) {
        int mode = 0;
        if (file.canRead()) mode |= 292;
        if (file.canWrite()) mode |= 144;
        if (file.canExecute()) mode |= 73;
        return mode;
    }
}