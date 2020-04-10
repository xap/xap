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


package com.gigaspaces.lrmi.nio.filters;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * A network filter to send compress messages between client and server.
 *
 * @author barak
 */

public class IOStreamCompressionFilter implements IOStreamFilter {

    private static final Logger logger = LoggerFactory.getLogger(IOStreamCompressionFilter.class.getName());
    private final Algo algo;

    public enum Algo {
        ZIP
    }

    public IOStreamCompressionFilter(Algo algo) {
        this.algo = algo;
        _compressor = new Deflater();
        _compressor.setLevel(Deflater.BEST_COMPRESSION);
        _decompressor = new Inflater();
    }

    /**
     * @param compressionLevel compresson level should be a number in (0-9)
     */
    public void setCompressionLevel(int compressionLevel) {
        _compressor.setLevel(compressionLevel);
    }

    private Deflater _compressor;
    private Inflater _decompressor;

    public byte[] unrwap(ByteBuffer buf) throws DataFormatException {
        _decompressor = new Inflater();
        int size = buf.remaining();
        _decompressor.setInput(toByteArray(buf));

        // Create an expandable byte array to hold the decompressed data
        ByteArrayOutputStream bos = new ByteArrayOutputStream(size);

        // Decompress the data
        byte[] b = new byte[1024];
        while (!_decompressor.finished()) {
            int count = _decompressor.inflate(b);
            bos.write(b, 0, count);
        }
        _decompressor.reset();
        byte[] res = bos.toByteArray();
        if (logger.isDebugEnabled()) {
            logger.debug("Uncompress message of " + size + " bytes to "
                    + res.length + " bytes, ratio ["
                    + (int) Math.ceil((size * 100.0) / res.length) + "%]");
        }
        // logger.info("Unraping result: " + Reader.toList(res));
        // Get the decompressed data
        return res;
    }

    private byte[] toByteArray(ByteBuffer buf) {
        byte[] res = new byte[buf.remaining()];
        buf.get(res, 0, res.length);
        return res;
    }

    public byte[] wrap(ByteBuffer buf) throws Exception {
        int size = buf.remaining();
        _compressor.setInput(toByteArray(buf));
        _compressor.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(size);
        byte[] b = new byte[1024];
        while (!_compressor.finished()) {
            int count = _compressor.deflate(b);
            bos.write(b, 0, count);
        }
        bos.close();
        _compressor.reset();
        // Get the compressed data
        byte[] res = bos.toByteArray();
        if (logger.isDebugEnabled()) {
            logger.debug("Compress message of " + size + " bytes to "
                    + res.length + " bytes, ratio ["
                    + (int) Math.ceil((res.length * 100.0) / size) + "%]");
        }
        return res;
    }


}
