package com.gigaspaces.sql.aggregatornode.netty.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.*;

class ServerBeanTest {
    public static final String SPACE_NAME = "mySpace";

    private GigaSpace gigaSpace;
    private ServerBean server;

    @BeforeEach
    void setUp() throws Exception {
        gigaSpace = new GigaSpaceConfigurer(
                new EmbeddedSpaceConfigurer(SPACE_NAME)
                        .addProperty("space-config.QueryProcessor.datetime_format", "yyyy-MM-dd HH:mm:ss.SSS")
        ).gigaSpace();


        for (int i = 0; i < 10; i++) {
            gigaSpace.write(new MyBean(i, "Value" + i));
        }

        server = new ServerBean(SPACE_NAME);
        server.init();
    }

    @Test
    void testConnection() throws Exception {
        Class.forName("org.postgresql.Driver");

        String url = "jdbc:postgresql://localhost/test?user=fred&password=secret";
        try (Connection conn = DriverManager.getConnection(url)) {
            assertFalse(conn.isClosed());
            assertTrue(conn.isValid(1000));
        }
    }

    @Test
    void test() throws Exception {
        Thread.sleep(Long.MAX_VALUE);
    }
}