package org.openspaces;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

@SpaceClass
public class Message {
    private String msg;
    private int size;

    public Message() {
    }

    public Message(String msg, int size) {
        this.msg = msg;
        this.size = size;
    }

    @SpaceId
    @SpaceIndex(type = SpaceIndexType.EQUAL, unique = true)
    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }


    @SpaceIndex(type = SpaceIndexType.ORDERED, unique = true)
    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "Message{" +
                "msg='" + msg + '\'' +
                ", size=" + size +
                '}';
    }
}
