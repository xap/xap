package my;

import my.output.DumpUtils;
import my.model.Model;
import org.openspaces.core .GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Test {
    public static void main(String[] args) {
        fillSpace();
        try (Connection conn = DriverManager.getConnection("jdbc:gigaspaces:aggregator")) {
            execute(conn, (String.format("SELECT * FROM %s where rowNum < 4", Model.class.getName())));
            execute(conn, (String.format("SELECT * FROM %s where rowNum < 4", Model.class.getName())));


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static void execute(Connection conn, String statement) throws SQLException {
        ResultSet res = conn.createStatement().executeQuery(statement);
        DumpUtils.dump(res);
    }

    private static void fillSpace() {
        SpaceProxyConfigurer configurer = new SpaceProxyConfigurer("mySpace").lookupGroups("yohanaPC");
        GigaSpace gigaSpace = new GigaSpaceConfigurer(configurer).gigaSpace();
        if (gigaSpace.count(null) != 0) return;
        for (int i = 0; i < 10; i++) {
            gigaSpace.write(new Model("Name" + i, i));
        }
    }
}
