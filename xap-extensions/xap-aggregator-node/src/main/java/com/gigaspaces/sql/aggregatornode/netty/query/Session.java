package com.gigaspaces.sql.aggregatornode.netty.query;

import java.nio.charset.Charset;
import java.util.TimeZone;

import static com.gigaspaces.sql.aggregatornode.netty.utils.Constants.*;

public class Session {
    private Charset charset = DEFAULT_CHARSET;
    private String dateStyle = DEFAULT_DATE_STYLE;
    private TimeZone timeZone = DEFAULT_TIME_ZONE;
    private String username = "";
    private String database = "";

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
}
