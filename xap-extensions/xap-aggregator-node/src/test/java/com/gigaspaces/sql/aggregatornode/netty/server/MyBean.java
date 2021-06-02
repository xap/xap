package com.gigaspaces.sql.aggregatornode.netty.server;

import com.gigaspaces.annotation.pojo.SpaceId;

public class MyBean {
    private Integer id;
    private String value;

    public MyBean() {
    }

    public MyBean(Integer id, String value) {
        this.id = id;
        this.value = value;
    }

    @SpaceId()
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
