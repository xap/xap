package com.gigaspaces.sql.aggregatornode.netty.server;

import org.junit.jupiter.api.BeforeAll;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractServerTest {
    public static final String SPACE_NAME = "mySpace";

    protected static GigaSpace gigaSpace;
    protected static ServerBean server;

    @BeforeAll
    static void setupServer() throws Exception {
        Class.forName("org.postgresql.Driver");
        gigaSpace = new GigaSpaceConfigurer(
                new EmbeddedSpaceConfigurer(SPACE_NAME)
                        .addProperty("space-config.QueryProcessor.datetime_format", "yyyy-MM-dd HH:mm:ss.SSS")
        ).gigaSpace();
        server = new ServerBean(SPACE_NAME);
        server.init();
    }

    protected Connection connect(boolean simple) throws Exception {
        String url = "jdbc:postgresql://localhost/test?user=fred&password=secret";
        if (simple)
            url += "&preferQueryMode=simple";

        final Connection conn = DriverManager.getConnection(url);
        assertFalse(conn.isClosed());
        assertTrue(conn.isValid(1000));
        return conn;
    }
}
