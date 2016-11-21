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

package org.openspaces.itest.persistency.cassandra.archive;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.j_spaces.core.IJSpace;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.itest.persistency.cassandra.CassandraTestServer;
import org.openspaces.persistency.cassandra.CassandraConsistencyLevel;
import org.openspaces.persistency.cassandra.archive.CassandraArchiveOperationHandler;
import org.openspaces.persistency.cassandra.archive.CassandraArchiveOperationHandlerConfigurer;
import org.openspaces.persistency.cassandra.error.SpaceCassandraException;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyRowMapper;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * GS-10785 Test Archive Container
 *
 * In order to run this test in eclise, edit the JUnit Test Configuration: Click the ClassPath tab,
 * click User Entries. Add Folders: Gigaspaces/ and Gigaspaces/src/java/resources and
 * openspaces/src/main/java/
 *
 * @author Itai Frenkel
 * @since 9.1.1
 */
public class CassandaraArchiveOperationHandlerTest {

    private static final String SPACE_DOCUMENT_NAME_KEY = "Name";

    private static final String SPACEDOCUMENT_NAME = "Anvil";

    private static final String SPACEDOCUMENT_TYPENAME = "Product";
    private static final String SPACEDOCUMENT2_TYPENAME = "Product2";

    private static final String SPACEDOCUMENT_ID = "hw-1234";
    private static final Long SPACEDOCUMENT2_ID = 1234l;

    private final Log logger = LogFactory.getLog(this.getClass());

    private final String TEST_NAMESPACE_XML = "/org/openspaces/itest/persistency/cassandra/archive/test-cassandra-archive-handler-namespace.xml";
    private final String TEST_RAW_XML = "/org/openspaces/itest/persistency/cassandra/archive/test-cassandra-archive-handler-raw.xml";

    private final CassandraTestServer server = new CassandraTestServer();

    private boolean skipRegisterTypeDescriptor;

    @Before
    public void startServer() {
        server.initialize(false);
    }

    @After
    public void stopServer() {
        server.destroy();
    }

    /**
     * Tests archiver with namespace spring bean xml
     */
    @Test
    public void testXmlRaw() {
        xmlTest(TEST_RAW_XML);
    }

    /**
     * Tests archiver with namespace spring bean xml
     */
    @Test
    public void testXmlNamespace() {
        xmlTest(TEST_NAMESPACE_XML);
    }

    @Test
    public void testConfigurer() throws Exception {
        configurerTest();
    }

    @Test(expected = SpaceCassandraException.class)
    public void testNoTypeDescriptorInSpace() throws Exception {
        skipRegisterTypeDescriptor = true;
        configurerTest();
    }

    private void configurerTest() throws Exception {
        final UrlSpaceConfigurer urlSpaceConfigurer = new UrlSpaceConfigurer("/./space");

        CassandraArchiveOperationHandler archiveHandler = null;

        try {
            GigaSpace gigaSpace;

            final IJSpace space = urlSpaceConfigurer.create();
            gigaSpace = new GigaSpaceConfigurer(space).create();

            archiveHandler =
                    new CassandraArchiveOperationHandlerConfigurer()
                            .keyspace(server.getKeySpaceName())
                            .hosts(server.getHost())
                            .port(server.getPort())
                            .gigaSpace(gigaSpace)
                            .create();

            test(archiveHandler, gigaSpace);
        } finally {

            if (urlSpaceConfigurer != null) {
                urlSpaceConfigurer.close();
            }

            if (archiveHandler != null) {
                archiveHandler.destroy();
            }
        }
    }

    private void xmlTest(String relativeXmlName) {

        final boolean refreshNow = false;
        final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{relativeXmlName}, refreshNow);

        PropertyPlaceholderConfigurer propertyConfigurer = new PropertyPlaceholderConfigurer();
        Properties properties = new Properties();
        properties.put("cassandra.keyspace", server.getKeySpaceName());
        properties.put("cassandra.hosts", server.getHost());
        properties.put("cassandra.port", "" + server.getPort());
        properties.put("cassandra.write-consistency", "ALL");
        propertyConfigurer.setProperties(properties);
        context.addBeanFactoryPostProcessor(propertyConfigurer);
        context.refresh();

        try {
            final CassandraArchiveOperationHandler archiveHandler = context.getBean(CassandraArchiveOperationHandler.class);
            Assert.assertEquals(CassandraConsistencyLevel.ALL, archiveHandler.getWriteConsistency());
            final GigaSpace gigaSpace = context.getBean(org.openspaces.core.GigaSpace.class);
            test(archiveHandler, gigaSpace);
        } finally {
            context.close();
        }
    }

    private void test(CassandraArchiveOperationHandler archiveHandler, GigaSpace gigaSpace) {

        if (!skipRegisterTypeDescriptor) {
            registerTypeDescriptor1(gigaSpace);
            registerTypeDescriptor2(gigaSpace);
        }

        archiveHandler.archive(createSpaceDocument1(), createSpaceDocument2());

        verifyDocument1InCassandra();
        verifyDocument2InCassandra();
    }

    private SpaceDocument createSpaceDocument1() {
        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("CatalogNumber", SPACEDOCUMENT_ID);
        properties.put("Category", "Hardware");
        properties.put(SPACE_DOCUMENT_NAME_KEY, SPACEDOCUMENT_NAME);
        properties.put("Price", 9.99f);
        final SpaceDocument document = new SpaceDocument(SPACEDOCUMENT_TYPENAME, properties);
        return document;
    }

    private SpaceDocument createSpaceDocument2() {
        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("CatalogNumber", SPACEDOCUMENT2_ID);
        properties.put("Category", "Hardware");
        properties.put(SPACE_DOCUMENT_NAME_KEY, SPACEDOCUMENT_NAME);
        properties.put("Price", 9.99f);
        final SpaceDocument document = new SpaceDocument(SPACEDOCUMENT2_TYPENAME, properties);
        return document;
    }

    private void registerTypeDescriptor1(GigaSpace gigaSpace) {
        final SpaceTypeDescriptor typeDescriptor =
                new SpaceTypeDescriptorBuilder(SPACEDOCUMENT_TYPENAME)
                        .addFixedProperty("CatalogNumber", String.class)
                        .idProperty("CatalogNumber")
                        .routingProperty("Category")
                        .addPropertyIndex(SPACE_DOCUMENT_NAME_KEY, SpaceIndexType.BASIC)
                        .addPropertyIndex("Price", SpaceIndexType.EXTENDED)
                        .create();
        gigaSpace.getTypeManager().registerTypeDescriptor(typeDescriptor);
    }

    private void registerTypeDescriptor2(GigaSpace gigaSpace) {
        final SpaceTypeDescriptor typeDescriptor =
                new SpaceTypeDescriptorBuilder(SPACEDOCUMENT2_TYPENAME)
                        .addFixedProperty("CatalogNumber", Long.class)
                        .idProperty("CatalogNumber")
                        .routingProperty("Category")
                        .addPropertyIndex(SPACE_DOCUMENT_NAME_KEY, SpaceIndexType.BASIC)
                        .addPropertyIndex("Price", SpaceIndexType.EXTENDED)
                        .create();
        gigaSpace.getTypeManager().registerTypeDescriptor(typeDescriptor);
    }

    private void verifyDocument1InCassandra() {

        Cluster cluster = HFactory.getOrCreateCluster("test-localhost_" + server.getPort(), server.getHost() + ":" + server.getPort());
        Keyspace keyspace = HFactory.createKeyspace(server.getKeySpaceName(), cluster);

        String columnFamilyName = SPACEDOCUMENT_TYPENAME; // as long as shorter than 40 bytes
        ThriftColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(
                keyspace,
                columnFamilyName,
                StringSerializer.get(),
                StringSerializer.get());

        Assert.assertTrue(SPACEDOCUMENT_TYPENAME + " does not exist", template.isColumnsExist(SPACEDOCUMENT_ID));

        ColumnFamilyRowMapper<String, String, Object> mapper = new ColumnFamilyRowMapper<String, String, Object>() {
            @Override
            public String mapRow(ColumnFamilyResult<String, String> rs) {

                for (String columnName : rs.getColumnNames()) {
                    ByteBuffer bytes = rs.getColumn(columnName).getValueBytes();
                    if (columnName.equals(SPACE_DOCUMENT_NAME_KEY)) {
                        return StringSerializer.get().fromByteBuffer(bytes);
                    }
                }

                return "Could not find column " + SPACE_DOCUMENT_NAME_KEY;
            }
        };
        Object name = template.queryColumns(SPACEDOCUMENT_ID, mapper);
        Assert.assertEquals(SPACEDOCUMENT_NAME, name);
    }

    private void verifyDocument2InCassandra() {

        Cluster cluster = HFactory.getOrCreateCluster("test-localhost_" + server.getPort(), server.getHost() + ":" + server.getPort());
        Keyspace keyspace = HFactory.createKeyspace(server.getKeySpaceName(), cluster);

        String columnFamilyName = SPACEDOCUMENT2_TYPENAME; // as long as shorter than 40 bytes
        ThriftColumnFamilyTemplate<Long, String> template = new ThriftColumnFamilyTemplate<Long, String>(
                keyspace,
                columnFamilyName,
                LongSerializer.get(),
                StringSerializer.get());

        Assert.assertTrue(SPACEDOCUMENT_TYPENAME + " does not exist", template.isColumnsExist(SPACEDOCUMENT2_ID));

        ColumnFamilyRowMapper<Long, String, Object> mapper = new ColumnFamilyRowMapper<Long, String, Object>() {
            @Override
            public String mapRow(ColumnFamilyResult<Long, String> rs) {

                for (String columnName : rs.getColumnNames()) {
                    ByteBuffer bytes = rs.getColumn(columnName).getValueBytes();
                    if (columnName.equals(SPACE_DOCUMENT_NAME_KEY)) {
                        return StringSerializer.get().fromByteBuffer(bytes);
                    }
                }

                return "Could not find column " + SPACE_DOCUMENT_NAME_KEY;
            }
        };
        Object name = template.queryColumns(SPACEDOCUMENT2_ID, mapper);
        Assert.assertEquals(SPACEDOCUMENT_NAME, name);
    }
}

