/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.start;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which can be used to print a descriptive warning message if a codebase accessibility
 * problem is found.
 *
 * @author Sun Microsystems, Inc.
 */
@com.gigaspaces.api.InternalApi
public class HTTPDStatus {
    /**
     * Config logger.
     */
    private static final Logger logger = ServiceStarter.logger;

    // Private constructor to prevent instantiation
    private HTTPDStatus() {
    }

    /**
     * Command line interface that checks the accessability of a desired JAR file(s), given its
     * <code>URL</code> address. Note: The provided <code>URL</code>(s) cannot contain embedded
     * spaces.
     *
     * @param args <code>String</code> array containing the command line arguments
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: HTTPDStatus URL1 [URL2 ... URLN]");
            return;
        }//endif
        for (int i = 0; i < args.length; i++) {
            httpdWarning(args[i]);
        }
    }//end main


    /**
     * Method that takes a <code>codebase</code> parameter and displays a warning message if it is
     * determined that a potential codebase accessibility problem exists.
     *
     * @param codebase <code>String</code> containing the codebase to poll for the existence of a
     *                 running HTTP server with access to the JAR file referenced in this parameter
     */
    public static void httpdWarning(String codebase) {
        if (codebase == null) {
            logger.warn("Problem accessing desired URL[{}]: {}.", codebase, "Codebase is null");
            return;
        }
        StringTokenizer st = new StringTokenizer(codebase, " ");
        String url = null;
        URL u = null;
        for (int i = 0; st.hasMoreTokens(); i++) {
            url = st.nextToken();
            try {
                u = new URL(url);
                String fileName = u.getFile();
                //Skip file check for directories
                if (fileName == null ||
                        fileName.endsWith("/")) {
                    logger.trace("Skipping file check for: {}", url);
                } else {
                    try {
                        drainStream(u.openStream());
                    } catch (Exception ioe) {
                        logger.warn("Problem accessing desired URL[{}]: {}.", url, ioe.toString());
                        logger.trace("Associated exception:", ioe);
                    }
                }
            } catch (MalformedURLException e) {
                logger.warn("Unknown protocol for URL: {} may cause problems", url);
                logger.trace("Associated exception:", e);
            }
        }
        return;
    }//end httpdStatus

    /**
     * Reads and discards all data from a given input stream.
     *
     * @param is the <code>InputStream</code> from which to read data
     */
    private static void drainStream(InputStream is) throws IOException {
        BufferedInputStream reader =
                new BufferedInputStream(is);
        while (reader.read() != -1) {
        }
    }//end drainSocket

}//end class HTTPDStatus
