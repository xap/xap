package my;

import com.gigaspaces.sql.aggregatornode.netty.client.output.DumpUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Test {
    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection("jdbc::aggregator")) {
            ResultSet res = conn.createStatement().executeQuery("SELECT * FROM com.j_spaces.examples.benchmark.messages.MessagePOJO where rowNum < 2");
            DumpUtils.dump(res);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
