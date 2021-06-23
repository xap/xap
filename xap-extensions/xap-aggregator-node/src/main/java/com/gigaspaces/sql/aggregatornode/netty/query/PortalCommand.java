package com.gigaspaces.sql.aggregatornode.netty.query;

enum PortalCommand {
    SHOW("SHOW"),
    SELECT("SELECT"),
    SET("SET");

    private final String tag;

    PortalCommand(String tag) {
        this.tag = tag;
    }

    public String tag() {
        return tag;
    }
}
