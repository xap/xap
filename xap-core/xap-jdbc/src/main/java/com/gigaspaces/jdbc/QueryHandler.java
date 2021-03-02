package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.jdbc.ResultEntry;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class QueryHandler {

    public ResponsePacket handle(String query, IJSpace space) {
        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            QueryExecutor qE = new QueryExecutor(space);
            ((Select) statement).getSelectBody().accept(qE);

            QueryResult res = qE.execute();
            ResponsePacket packet = new ResponsePacket();
            packet.setResultEntry(convertEntriesToResultArrays(res));
            return packet;
        } catch (JSQLParserException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }


        return null;
    }

    public ResultEntry convertEntriesToResultArrays(QueryResult entries) {
        // Column (field) names and labels (aliases)
        int columns = entries.getQueryColumns().size();

        String[] fieldNames = entries.getQueryColumns().stream().map(QueryColumn::getName).toArray(String[]::new);
        String[] columnLabels = entries.getQueryColumns().stream().map(qC -> qC.getAlias() == null ? qC.getName() : qC.getAlias()).toArray(String[]::new);

        //the field values for the result
        Object[][] fieldValues = new Object[entries.size()][columns];

        Iterator<TableEntry> iter = entries.iterator();

        int row = 0;

        while (iter.hasNext()) {
            TableEntry entry = iter.next();

            int column = 0;
            for (int i = 0; i < columns; i++) {
                fieldValues[row][column++] = entry.getPropertyValue(i);
            }

            row++;
        }


        return new ResultEntry(
                fieldNames,
                columnLabels,
                null,
                fieldValues);
    }
}
