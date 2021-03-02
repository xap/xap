package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ProjectionTemplate;
import com.gigaspaces.jdbc.QueryResult;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.jdbc.SQLUtil;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.QueryTableData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ConcreteTableContainer extends TableContainer {
    private final IJSpace space;
    private final QueryTemplatePacket queryTemplatePacket;
    private final ITypeDesc typeDesc;

    private final List<QueryColumn> visibleColumns = new ArrayList<>();

    public ConcreteTableContainer(String name, String alias, IJSpace space) {
        this.space = space;

        try {
            typeDesc = SQLUtil.checkTableExistence(name, space);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Unknown type [" + name + "]");
        }

        QueryTableData queryTableData = new QueryTableData(name, alias, 0);
        queryTableData.setTypeDesc(typeDesc);
        queryTemplatePacket = new QueryTemplatePacket(queryTableData, QueryResultTypeInternal.NOT_SET);
    }

    @Override
    public QueryResult getResult() throws SQLException {
        String[] projectionC = visibleColumns.stream().map(QueryColumn::getName).toArray(String[]::new);

        try {
            ProjectionTemplate _projectionTemplate = ProjectionTemplate.create(projectionC, typeDesc);
            queryTemplatePacket.setProjectionTemplate(_projectionTemplate);
            IQueryResultSet<IEntryPacket> res = queryTemplatePacket.readMultiple(space.getDirectProxy(), null, 100, ReadModifiers.REPEATABLE_READ);
            return new QueryResult(res, visibleColumns);
        } catch (Exception e) {
            throw new SQLException("Failed to get results from space", e);
        }
    }

    @Override
    public void addColumn(String columnName, String alias) {
        if (typeDesc.getFixedPropertyPositionIgnoreCase(columnName) == -1) {
            throw new IllegalArgumentException("Could not find column with name");
        }
        this.visibleColumns.add(new QueryColumn(columnName, alias, true));
    }
}
