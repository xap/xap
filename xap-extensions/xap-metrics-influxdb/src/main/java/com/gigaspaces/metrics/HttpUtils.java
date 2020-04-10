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

package com.gigaspaces.metrics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
public class HttpUtils {
    private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class.getName());

    private static final Charset DEFAULT_ENCODING = Charset.forName("UTF-8");

    public static int post(URL url, String content, String contentType, int timeout) throws IOException {
        final byte[] data = content.getBytes(DEFAULT_ENCODING);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            // Configure connection:
            connection.setRequestProperty("Content-Length", String.valueOf(data.length));
            connection.setRequestProperty("Content-Type", contentType);
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(timeout);
            connection.setReadTimeout(timeout);
            connection.setDoOutput(true);
            // Write output:
            OutputStream output = connection.getOutputStream();
            output.write(data);
            output.flush();
            // Get response:
            final int responseCode = connection.getResponseCode();
            final String error = read(connection.getErrorStream());
            if (!isValid(responseCode) || error != null)
                throw new IOException("Posting to " + url + " returned HTTP code " + responseCode +
                        " '" + connection.getResponseMessage() + "' with the following error: " + error);

            return responseCode;
        } finally {
            connection.disconnect();
        }
    }

    public static String get(URL url, String contentType, int timeout) throws IOException {

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            // Configure connection:
            connection.setRequestProperty("Content-Type", contentType);//"application/json"
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(timeout);
            connection.setReadTimeout(timeout);
            connection.setDoOutput(true);

            // Get response:
            final int responseCode = connection.getResponseCode();
            final String error = read(connection.getErrorStream());
            if (!isValid(responseCode) || error != null)
                throw new IOException("Getting from " + url + " returned HTTP code " + responseCode +
                                      " '" + connection.getResponseMessage() + "' with the following error: " + error);

            //read input
            InputStream inputStream = connection.getInputStream();
            byte[] tmp = new byte[1024];
            StringBuilder strBuilder = new StringBuilder();
            while (inputStream.available() > 0) {
                int i = inputStream.read(tmp, 0, 1024);
                if (i < 0) {
                    break;
                }
                strBuilder.append(new String(tmp, 0, i));
            }

            return strBuilder.toString();
        } finally {
            connection.disconnect();
        }
    }

    private static boolean isValid(int responseCode) {
        return responseCode >= 200 && responseCode < 300;
    }

    private static String read(InputStream stream) {
        if (stream == null)
            return null;
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        String line;
        try {
            while ((line = br.readLine()) != null)
                sb.append(line);
        } catch (IOException e) {
            if (logger.isTraceEnabled())
                logger.trace("Failed to read HTTP response error stream", e);
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                if (logger.isTraceEnabled())
                    logger.trace("Failed to close HTTP response error stream", e);
            }
        }
        return sb.toString();
    }
}
