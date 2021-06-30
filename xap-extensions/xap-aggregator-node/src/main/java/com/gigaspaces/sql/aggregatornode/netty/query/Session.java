package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceFinder;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.TimeZone;

import static com.gigaspaces.sql.aggregatornode.netty.utils.Constants.*;

public class Session implements Closeable {
    private Charset charset = DEFAULT_CHARSET;
    private String dateStyle = DEFAULT_DATE_STYLE;
    private TimeZone timeZone = DEFAULT_TIME_ZONE;
    private String username = "";
    private String database = "";
    private ISpaceProxy space;

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public String getDateStyle() {
        return dateStyle;
    }

    public void setDateStyle(String dateStyle) {
        this.dateStyle = dateStyle;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public ISpaceProxy getSpace() {
        if (database == null || database.length() == 0) {
            throw new RuntimeException("Space name is not provided");
        }
        if (space == null) {
            try {
                space = (ISpaceProxy) SpaceFinder.find("jini://localhost/*/" + database);
            } catch (FinderException e) {
                throw new RuntimeException("Could not find space", e);
            }
        }
        return space;
    }

    @Override
    public void close() {
        if (space != null)
            space.close();
    }
}
