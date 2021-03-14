package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.explainplan.ExplainPlanImpl;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ProjectionTemplate;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.exceptions.TypeNotFoundException;
import com.gigaspaces.jdbc.model.result.ExplainPlanResult;
import com.gigaspaces.query.explainplan.ExplainPlan;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.jdbc.SQLUtil;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.QueryTableData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.gigaspaces.internal.query.explainplan.ExplainPlanUtil.notEmpty;

public class ConcreteTableContainer extends TableContainer {
    private final IJSpace space;
    private final QueryTemplatePacket queryTemplatePacket;
    private final ITypeDesc typeDesc;
    private final int maxResults = Integer.MAX_VALUE;

    private final List<QueryColumn> visibleColumns = new ArrayList<>();
    private final String name;
    private final String alias;

    public ConcreteTableContainer(String name, String alias, IJSpace space) {
        this.space = space;
        this.name = name;
        this.alias = alias;

        try {
            typeDesc = SQLUtil.checkTableExistence(name, space);
        } catch (SQLException e) {
            throw new TypeNotFoundException("Unknown table [" + name + "]", e);
        }

        queryTemplatePacket = createQueryTemplatePacket(name);
    }

    private QueryTemplatePacket createQueryTemplatePacket(String tableName) {
        QueryTableData queryTableData = new QueryTableData(tableName, null, 0);
        queryTableData.setTypeDesc(typeDesc);
        return new QueryTemplatePacket(queryTableData, QueryResultTypeInternal.NOT_SET);
    }

    @Override
    public QueryResult executeRead(boolean explainPlan) throws SQLException {
        String[] projectionC = visibleColumns.stream().map(QueryColumn::getName).toArray(String[]::new);

        try {
            ProjectionTemplate _projectionTemplate = ProjectionTemplate.create(projectionC, typeDesc);
            queryTemplatePacket.setProjectionTemplate(_projectionTemplate);

            int modifiers = ReadModifiers.REPEATABLE_READ;

            ExplainPlan explainPlanImpl = null;
            if (explainPlan) {
                String columns = "";
                for (QueryColumn column : visibleColumns) {
                    columns += column.getName() + (notEmpty(column.getAlias()) ? " as "+column.getAlias()+" " : " ");
                }

                explainPlanImpl = new ExplainPlanImpl(name, alias, columns.trim());
                queryTemplatePacket.setExplainPlan(explainPlanImpl);
                modifiers = Modifiers.add(modifiers, Modifiers.EXPLAIN_PLAN);
                modifiers = Modifiers.add(modifiers, Modifiers.DRY_RUN);
            }

            IQueryResultSet<IEntryPacket> res = queryTemplatePacket.readMultiple(space.getDirectProxy(), null, maxResults, modifiers);
            if (explainPlan) return new ExplainPlanResult(explainPlanImpl.toString());
            return new QueryResult(res, visibleColumns);
        } catch (Exception e) {
            throw new SQLException("Failed to get results from space", e);
        }
    }

    @Override
    public QueryColumn addQueryColumn(String columnName, String alias) {
        if (typeDesc.getFixedPropertyPositionIgnoreCase(columnName) == -1) {
            throw new ColumnNotFoundException("Could not find column with name [" + columnName + "]");
        }
        QueryColumn qc = new QueryColumn(columnName, alias, true);
        this.visibleColumns.add(qc);
        return qc;
    }

    public List<QueryColumn> getVisibleColumns() {
        return visibleColumns;
    }

    @Override
    public List<String> getAllColumnNames() {
        return Arrays.asList(typeDesc.getPropertiesNames());
    }

    @Override
    public String getTableNameOrAlias() {
        return alias == null ? name : alias;
    }
}
