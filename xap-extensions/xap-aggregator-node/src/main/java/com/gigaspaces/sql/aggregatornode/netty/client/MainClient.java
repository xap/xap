package com.gigaspaces.sql.aggregatornode.netty.client;

import com.gigaspaces.sql.aggregatornode.netty.client.output.DumpUtils;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.jdbc.driver.GResultSet;

public class MainClient {

    public static void main(String[] args) throws Exception {
        EchoClient client = new EchoClient();
        client.init();

        ResponsePacket res = client.send("SELECT * FROM com.j_spaces.examples.benchmark.messages.MessagePOJO").get();
        GResultSet resultSet = new GResultSet(null, res.getResultEntry());

        DumpUtils.dump(resultSet);



        res = client.send("SELECT * FROM com.j_spaces.examples.benchmark.messages.MessagePOJO where rownum < 2").get();
        resultSet = new GResultSet(null, res.getResultEntry());

        DumpUtils.dump(resultSet);
    }
}
