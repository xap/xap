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
package com.gigaspaces.attribute_store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Note that this is a technology preview feature may be changed in the future.
 *
 * @author Kobi, Modified by Barak.
 * @since 10.2
 */
@com.gigaspaces.api.InternalApi
public class PropertiesFileAttributeStore implements AttributeStore, Serializable {
    private static final long serialVersionUID = 1789885876431145300L;

    final private static Logger logger = LoggerFactory.getLogger(PropertiesFileAttributeStore.class.getName());

    private File file;

    @SuppressWarnings("unused")
    public PropertiesFileAttributeStore() {
    }

    public PropertiesFileAttributeStore(String path) {
        file = new File(path);
        File directory = file.getParentFile();
        if (!directory.exists())
            //noinspection ResultOfMethodCallIgnored
            directory.mkdirs();
        logger.info("PropertiesFileAttributeStore saved at " + directory.getAbsolutePath());
    }

    @Override
    public String get(String key) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            FileChannel fileChannel = randomAccessFile.getChannel();
            FileLock fileLock = fileChannel.lock();
            try {
                Properties p = readPropertiesFromChannel(fileChannel);
                return p.getProperty(key);
            } finally {
                fileLock.release();
                fileChannel.close();
            }
        }
    }

    @Override
    public String set(String key, String value) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            FileChannel fileChannel = randomAccessFile.getChannel();
            FileLock fileLock = fileChannel.lock();
            String oldValue = null;
            try {
                Properties p = readPropertiesFromChannel(fileChannel);
                oldValue = (String) p.setProperty(key, value);
                writePropertiesToChannel(fileChannel, p);
            } finally {
                fileLock.release();
                fileChannel.close();
            }
            return oldValue;
        }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public String remove(String name) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            FileChannel fileChannel = randomAccessFile.getChannel();
            FileLock fileLock = fileChannel.lock();
            String oldValue = null;
            try {
                Properties p = readPropertiesFromChannel(fileChannel);
                oldValue = (String) p.remove(name);
                writePropertiesToChannel(fileChannel, p);
            } finally {
                fileLock.release();
                fileChannel.close();
            }
            return oldValue;
        }
    }

    @Override
    public SharedLock getSharedLock(String key) {
        throw new UnsupportedOperationException("only supported in ZooKeeperAttributeStore.class");
    }

    public <T> T withProperties(PropertiesHandler<T> propertiesHandler) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            FileChannel fileChannel = randomAccessFile.getChannel();
            FileLock fileLock = fileChannel.lock();
            T ret = null;
            try {
                Properties p = readPropertiesFromChannel(fileChannel);
                ret = propertiesHandler.handle(p);
                writePropertiesToChannel(fileChannel, p);
            } finally {
                fileLock.release();
                fileChannel.close();
            }
            return ret;
        }
    }

    private Properties readPropertiesFromChannel(FileChannel fileChannel) throws IOException {
        int size = (int) fileChannel.size();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        while (0 < buffer.remaining()) {
            fileChannel.read(buffer);
        }
        Properties p = new Properties();
        p.load(new ByteArrayInputStream(buffer.array()));
        return p;
    }

    private void writePropertiesToChannel(FileChannel fileChannel, Properties p) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        p.store(os, null);
        byte[] byteArray = os.toByteArray();
        fileChannel.position(0);
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);
        while (0 < buffer.remaining()) {
            fileChannel.write(buffer);
        }
        fileChannel.truncate(byteArray.length);
    }

    @Override
    public byte[] getBytes(String key) throws IOException {
        throw new UnsupportedOperationException("only supported in ZooKeeperAttributeStore.class");
    }

    @Override
    public byte[] setBytes(String key, byte[] value) throws IOException {
        throw new UnsupportedOperationException("only supported in ZooKeeperAttributeStore.class");
    }



}
