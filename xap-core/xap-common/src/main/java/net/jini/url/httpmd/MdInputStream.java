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

package net.jini.url.httpmd;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.MessageDigest;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An input stream that checks the contents of another input stream against a message digest. The
 * stream throws WrongMessageDigestException if the end of the other input stream is reached and the
 * contents did not have the expected message digest. The message digest is not checked unless the
 * end of the input stream is reached.
 *
 * @author Sun Microsystems, Inc.
 */
class MdInputStream extends InputStream {

    /**
     * Logger.
     */
    private static final Logger logger =
            Logger.getLogger("net.jini.url.httpmd");

    /**
     * The size buffer to use when skipping input.
     */
    private static final int SKIP_BUFFER_SIZE = 512;

    /**
     * The HTTPMD URL associated with the input stream being checked.
     */
    private final URL url;

    /**
     * The input stream whose contents should be checked.
     */
    private final InputStream in;

    /**
     * The object to use to compute the message digest of the stream contents.
     */
    private final MessageDigest messageDigest;

    /**
     * The expected digest.
     */
    private final byte[] expectedDigest;

    /**
     * Set to the buffer to use when skipping input.
     */
    private byte[] skipBuffer;

    /**
     * Set to true when the contents have been checked against the message digest.
     */
    private boolean checked;

    /**
     * The exception message if the message digest was incorrect, or null if no exception should be
     * thrown.
     */
    private String failed;

    /**
     * Creates an input stream that checks the contents of another input stream against a message
     * digest. The stream throws WrongMessageDigestException if the end of the other input stream is
     * reached and the contents did not have the expected message digest.
     *
     * @param url            the HTTPMD URL associated with the input stream being checked
     * @param in             the input stream whose contents should be checked
     * @param messageDigest  the object to use for computing the message digest of the stream
     *                       contents
     * @param expectedDigest the expected message digest of the stream contents
     * @throws NullPointerException if any of the arguments is null
     */
    MdInputStream(URL url,
                  InputStream in,
                  MessageDigest messageDigest,
                  byte[] expectedDigest) {
        if (url == null || in == null || messageDigest == null) {
            throw new NullPointerException();
        }
        this.url = url;
        this.in = in;
        this.messageDigest = messageDigest;
        this.expectedDigest = expectedDigest.clone();
    }

    public synchronized int read() throws IOException {
        checkFailed();
        int result = in.read();
        if (result < 0) {
            checkDigest();
        } else {
            messageDigest.update((byte) result);
        }
        return result;
    }

    /**
     * Throws an exception if the message digest was found to be incorrect. Call this method before
     * performing any I/O operations to insure that the same exception continues to be thrown after
     * an incorrect message digest is discovered.
     */
    private void checkFailed() throws WrongMessageDigestException {
        if (failed != null) {
            WrongMessageDigestException exception =
                    new WrongMessageDigestException(failed);
            logger.log(Level.FINE, "Incorrect message digest", exception);
            throw exception;
        }
    }

    /**
     * Checks the message digest. Call this method when the end of the other input stream is
     * reached.
     */
    private void checkDigest() throws WrongMessageDigestException {
        if (!checked) {
            byte[] result = messageDigest.digest();
            checked = true;
            if (!MessageDigest.isEqual(result, expectedDigest)) {
                failed = "Incorrect message digest for " + url + ": " +
                        HttpmdUtil.digestString(result);
                checkFailed();
            }
        }
    }

    public synchronized int read(byte b[], int off, int len)
            throws IOException {
        checkFailed();
        int n = in.read(b, off, len);
        if (n < 0) {
            checkDigest();
        } else {
            messageDigest.update(b, off, n);
        }
        return n;
    }

    public synchronized long skip(long n) throws IOException {
        if (skipBuffer == null) {
            skipBuffer = new byte[SKIP_BUFFER_SIZE];
        }
        long remaining = n;
        while (remaining > 0) {
            int nr = read(
                    skipBuffer, 0, (int) Math.min(SKIP_BUFFER_SIZE, remaining));
            if (nr < 0) {
                break;
            }
            remaining -= nr;
        }
        return n - remaining;
    }

    public synchronized int available() throws IOException {
        checkFailed();
        return in.available();
    }

    public synchronized void close() throws IOException {
        checkFailed();
        in.close();
    }
}
