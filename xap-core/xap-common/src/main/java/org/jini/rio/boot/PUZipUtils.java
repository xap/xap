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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class PUZipUtils {

    final private static Logger logger = Logger.getLogger("com.gigaspaces.zip");

    public static class DownloadProcessingUnitException extends Exception {
        private static final long serialVersionUID = -8734985356212361134L;

        public DownloadProcessingUnitException(String message) {
            super(message);
        }

        public DownloadProcessingUnitException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final Random random = new Random();

    public static void uploadProcessingUnit(String deployURL, String puPath, File puFile) throws Exception {
        if (!puFile.exists()) {
            throw new FileNotFoundException("File " + puFile.getAbsolutePath() +" doesn't exist");
        }
        boolean deletePUFile = false;
        if (puFile.isDirectory() && puFile.getParent().endsWith("/templates")) {
            logger.finest("Skip upload of " + puFile + " - template file");
            return;
        }
        if (puFile.exists() && puFile.isDirectory()) {
            // this is a directory, jar it up and prepare it for upload
            File zipPUFile = new File(System.getProperty("java.io.tmpdir") + File.separator + puPath + ".zip");
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Zip directory [" + puFile.getAbsolutePath() + "] into [" + zipPUFile.getAbsolutePath() + "]");
            }
            PUZipUtils.zip(puFile, zipPUFile);
            puFile = zipPUFile;
            deletePUFile = true;
        }
        if (!puFile.getName().endsWith(".zip")
                && !puFile.getName().endsWith(".jar")
                && !puFile.getName().endsWith(".war")) {
            throw new IllegalArgumentException("File " + puFile.getAbsolutePath() +" should be one of: zip/jar/war");
        }
        if (puFile.length() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("File " + puFile.getPath() + " is too big: " + puFile.length() + " bytes");
        }
        byte[] buffer = new byte[4098];
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Uploading [" + puPath + "] from [" + puFile.getPath() + "] to [" + deployURL + "]");
        }
        HttpURLConnection conn = (HttpURLConnection) new URL(deployURL + puFile.getName()).openConnection();
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setAllowUserInteraction(false);
        conn.setUseCaches(false);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Extract", "true");
        //Sets the Content-Length request property
        //And disables buffering of file in memory (pure streaming)
        conn.setFixedLengthStreamingMode((int) puFile.length());
        try {
            conn.connect();
            OutputStream out = new BufferedOutputStream(conn.getOutputStream());
            InputStream in = new BufferedInputStream(new FileInputStream(puFile));
            try {
                int byteCount = 0;
                int bytesRead = -1;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                    byteCount += bytesRead;
                }
                out.flush();
            } finally {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            }

            int responseCode = conn.getResponseCode();

            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            try {
                String line;
                StringBuffer sb = new StringBuffer();
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }

                if (responseCode != 200 && responseCode != 201) {
                    throw new RuntimeException("Failed to upload file, response code [" + responseCode + "], response: " + sb.toString());
                }

            } finally {
                reader.close();
            }
        } finally {
            conn.disconnect();

            if (deletePUFile) {
                puFile.delete();
            }
        }
        if (logger.isLoggable(Level.FINEST)) {
            logger.finest("Upload of [" + puPath + "] completed");
        }
    }

    public static long downloadProcessingUnit(String puName, URL url, File extractToTarget, File tempLocation) throws Exception {

        File tempFile = new File(tempLocation, puName.replace('\\', '/') + random.nextLong() + ".zip");

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Downloading [" + puName + "] from [" + url.toString() + "] to [" + tempFile.getAbsolutePath() + "], extracted to [" + extractToTarget.getAbsolutePath() + "]");
        }

        long size = 0;

        try {
            downloadProcessingUnit(url, tempFile, puName);

            size = tempFile.length();

            try {
                PUZipUtils.unzip(tempFile, extractToTarget);
            } catch (Exception e) {
                throw new DownloadProcessingUnitException("Failed to extract processing unit [" + puName + "] downloaded temp zip file from [" + tempFile.getAbsolutePath() + "] into [" + extractToTarget.getAbsolutePath() + "] zipfile size is " + size + " original download URL is " + url, e);
            }
        } finally {
            tempFile.delete();
        }

        return size;
    }

    public static void downloadProcessingUnit(URL url, File target, String puName) throws DownloadProcessingUnitException {
        TLSUtils.enableHttpsClient();
        HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            throw new DownloadProcessingUnitException("Failed to connect to [" + url.toString() + "] in order to download processing unit [" + puName + "]", e);
        }
        conn.setRequestProperty("Package", "true");
        int responseCode;
        try {
            responseCode = conn.getResponseCode();
        } catch (IOException e) {
            try {
                conn.disconnect();
            } catch (Exception e1) {
                // ignore
            }
            throw new DownloadProcessingUnitException("Failed to read response code from [" + url.toString() + "] in order to download processing unit [" + puName + "]", e);
        }
        if (responseCode != 200 && responseCode != 201) {
            try {
                if (responseCode == 404) {
                    throw new DownloadProcessingUnitException("Processing Unit [" + puName + "] not found on server [" + url.toString() + "]");
                }
                StringBuilder sb = new StringBuilder();
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }
                    reader.close();
                } catch (Exception e) {
                    // ignore this exception, failed to read input
                }
                throw new DownloadProcessingUnitException("Failed to connect/download (failure on the web server side) from  [" + url.toString() + "], response code [" + responseCode + "], response [" + sb.toString() + "]");
            } finally {
                conn.disconnect();
            }
        }

        target.getParentFile().mkdirs();

        try {
            InputStream in = new BufferedInputStream(conn.getInputStream());
            OutputStream out = new FileOutputStream(target);
            try {
                int byteCount = 0;
                byte[] buffer = new byte[4098];
                int bytesRead = -1;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                    byteCount += bytesRead;
                }
                out.flush();
            } finally {
                try {
                    in.close();
                } catch (IOException ex) {
                    // ignore
                }
                try {
                    out.close();
                } catch (IOException ex) {
                    // ignore
                }
            }
            conn.disconnect();

            RandomAccessFile ras = new RandomAccessFile(target, "rw");
            ras.getFD().sync();
            ras.close();
        } catch (IOException e) {
            throw new DownloadProcessingUnitException("Failed to read processing unit [" + puName + "] from [" + url.toString() + "] into [" + target.getAbsolutePath() + "]", e);
        } finally {
            try {
                conn.disconnect();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public static long unzip(File targetZip, File dirToExtract) throws Exception {
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Unzipping file [" + targetZip.getAbsolutePath() + "] with size [" + targetZip.length() + "] to [" + dirToExtract.getAbsolutePath() + "]");
        }
        long totalSize = 0;
        final int bufferSize = 4098;
        byte data[] = new byte[bufferSize];
        ZipFile zipFile = new ZipFile(targetZip);
        Enumeration<? extends ZipEntry> e = zipFile.entries();
        while (e.hasMoreElements()) {
            ZipEntry entry = e.nextElement();
            if (entry.isDirectory()) {
                File dir = new File(dirToExtract.getAbsolutePath() + "/" + entry.getName().replace('\\', '/'));
                for (int i = 0; i < 5; i++) {
                    dir.mkdirs();
                }
            } else {
                BufferedInputStream is = new BufferedInputStream(zipFile.getInputStream(entry));
                int count;
                File file = new File(dirToExtract.getAbsolutePath() + "/" + entry.getName().replace('\\', '/'));
                if (file.getParentFile() != null) {
                    file.getParentFile().mkdirs();
                }
                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest("Extracting zip entry [" + entry.getName() + "] with size [" + entry.getSize() + "] to [" + file.getAbsolutePath() + "]");
                }
                FileOutputStream fos = new FileOutputStream(file);
                BufferedOutputStream dest = new BufferedOutputStream(fos, bufferSize);
                while ((count = is.read(data, 0, bufferSize)) != -1) {
                    totalSize += count;
                    dest.write(data, 0, count);
                }
                dest.flush();
                dest.close();
                is.close();

                // sync the file to the file system
                RandomAccessFile ras = new RandomAccessFile(file, "rw");
                try {
                    ras.getFD().sync();
                } finally {
                    try {
                        ras.close();
                    } catch (Exception e1) {
                        // ignore
                    }
                }
            }
        }
        zipFile.close();
        return totalSize;
    }

    public static void zip(File dir2zip, File zipFile) throws Exception {
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile));
        internalZipDir(dir2zip.getAbsolutePath(), dir2zip.getAbsolutePath(), zos);
        zos.close();
    }

    private static void internalZipDir(String baseDir2zip, String dir2zip, ZipOutputStream zos) throws Exception {
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
        byte[] readBuffer = new byte[2156];
        int bytesIn = 0;
        //loop through dirList, and zip the files
        for (int i = 0; i < dirList.length; i++) {
            File f = new File(zipDir, dirList[i]);
            if (f.isDirectory()) {
                //if the File object is a directory, call this
                //function again to add its content recursively
                String filePath = f.getAbsolutePath();
                internalZipDir(baseDir2zip, filePath, zos);
                //loop again
                continue;
            }
            //if we reached here, the File object f was not a directory
            //create a FileInputStream on top of f
            FileInputStream fis = new FileInputStream(f);
            // create a new zip entry
            String path = f.getAbsolutePath().substring(baseDir2zip.length() + 1);
            path = path.replace('\\', '/');
            ZipEntry anEntry = new ZipEntry(path);
            if (logger.isLoggable(Level.FINEST)) {
                logger.finest("Compressing zip entry [" + anEntry.getName() + "] with size [" + anEntry.getSize() + "] at [" + f.getAbsolutePath() + "]");
            }
            //place the zip entry in the ZipOutputStream object
            zos.putNextEntry(anEntry);
            //now write the content of the file to the ZipOutputStream
            while ((bytesIn = fis.read(readBuffer)) != -1) {
                zos.write(readBuffer, 0, bytesIn);
            }
            //close the Stream
            fis.close();
        }
    }
}
