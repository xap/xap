package com.gigaspaces.sql.aggregatornode.netty.query;

import com.fasterxml.jackson.databind.util.ArrayIterator;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.jdbc.QueryHandler;
import com.gigaspaces.jdbc.calcite.GSOptimizer;
import com.gigaspaces.jdbc.calcite.GSRelNode;
import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.utils.PgType;
import com.j_spaces.jdbc.ResponsePacket;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings({"unchecked", "rawtypes"})
public class QueryProviderImpl implements QueryProvider{
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(QueryProviderImpl.class);

    private static final int[] ALL_TEXT = new int[0];
    private static final String EMPTY_NAME = "";

    private final ISpaceProxy space;
    private final QueryHandler handler;

    private final Map<String, StatementImpl> statements = new HashMap<>();
    private final Map<String, PortalImpl<?>> portals = new HashMap<>();

    public QueryProviderImpl(ISpaceProxy space) {
        this.space = space;
        this.handler = new QueryHandler();
    }

    public void init() throws ProtocolException {

    }

    @Override
    public void prepare(String stmt, String qry, int[] paramTypes) throws ProtocolException {
        if (stmt.isEmpty())
            statements.remove(stmt);
        else if (statements.containsKey(stmt))
            throw new NonBreakingException("26000", "duplicate statement name");

        try {
            statements.put(stmt, prepareStatement(stmt, qry, paramTypes));
        } catch (Exception e) {
            throw new NonBreakingException("42000",	"failed to prepare statement", e);
        }
    }


    @Override
    public void bind(String portal, String stmt, Object[] params, int[] formatCodes) throws ProtocolException {
        if (!statements.containsKey(stmt))
            throw new NonBreakingException("26000", "invalid statement name");

        if (portal.isEmpty())
            portals.remove(stmt);
        else if (portals.containsKey(portal))
            throw new NonBreakingException("34000", "duplicate cursor name");

        try {
            portals.put(portal, preparePortal(portal, statements.get(stmt), null, params, formatCodes));
        } catch (ProtocolException e) {
            throw e;
        } catch (Exception e) {
            throw new NonBreakingException("42000",	"failed to bind statement to a portal", e);
        }
    }

    @Override
    public StatementDescription describeS(String stmt) throws ProtocolException {
        if (!statements.containsKey(stmt))
            throw new NonBreakingException("26000", "invalid statement name");

        return statements.get(stmt).getDescription();
    }

    @Override
    public RowDescription describeP(String portal) throws ProtocolException {
        if (!portals.containsKey(portal))
            throw new NonBreakingException("34000", "invalid cursor name");

        return portals.get(portal).getDescription();
    }

    @Override
    public Portal<?> execute(String portal) throws ProtocolException {
        if (!portals.containsKey(portal))
            throw new NonBreakingException("34000", "invalid cursor name");

        return portals.get(portal);
    }

    @Override
    public void closeS(String name) throws ProtocolException {
        List<String> toClose = portals.values().stream()
                .filter(p -> p.getStatement().getName().equals(name))
                .map(Portal::name)
                .collect(Collectors.toList());

        for (String portal : toClose)
            closeP(portal);

        statements.remove(name);
    }

    @Override
    public void closeP(String name) throws ProtocolException {
        portals.remove(name);
    }

    @Override
    public void cancel(int pid, int secret) {
        // todo
    }

    @Override
    public List<LazyPortal<?>> executeQueryMultiline(String query) {
        GSOptimizer optimizer = new GSOptimizer(space);
        SqlNodeList nodes = optimizer.parseMultiline(query);
        List<LazyPortal<?>> result = new ArrayList<>();
        for (SqlNode node : nodes) {
            SqlNode validated = optimizer.validate(node);
            StatementDescription description = describe(optimizer, validated, ALL_TEXT);
            StatementImpl statement = new StatementImpl(EMPTY_NAME, validated, description);
            result.add(() -> (Portal) preparePortal(EMPTY_NAME, statement, optimizer, new Object[0], new int[0]));
        }
        return result;
    }

    private StatementImpl prepareStatement(String name, String query, int[] paramTypes) {
        GSOptimizer optimizer = new GSOptimizer(space);
        SqlNode sqlNode = optimizer.parse(query);
        SqlNode validated = optimizer.validate(sqlNode);
        StatementDescription description = describe(optimizer, validated, paramTypes);
        return new StatementImpl(name, sqlNode, description);
    }

    private StatementDescription describe(GSOptimizer optimizer, SqlNode queryAst, int[] paramTypes) {
        RelDataType columns = optimizer.extractRowType(queryAst);
        ParametersDescription paramDesc;
        if (paramTypes == null || paramTypes.length == 0) {
            List<ParameterDescription> params0 = new ArrayList<>();
            RelDataType params = optimizer.extractParameterType(queryAst);
            for (RelDataTypeField f : params.getFieldList()) {
                params0.add(new ParameterDescription(PgType.fromInternal(f.getType())));
            }
            paramDesc = new ParametersDescription(params0);
        } else {
            paramDesc = new ParametersDescription(paramTypes);
        }

        List<ColumnDescription> columns0 = new ArrayList<>();
        for (RelDataTypeField f : columns.getFieldList()) {
            columns0.add(new ColumnDescription(f.getName(), PgType.fromInternal(f.getType())));
        }
        RowDescription rowDesc = new RowDescription(columns0);
        return new StatementDescription(paramDesc, rowDesc);
    }

    private PortalImpl<?> preparePortal(String name, StatementImpl statement, GSOptimizer optimizer, Object[] params, int[] formatCodes) throws ProtocolException {
        try {
            SqlNode query = statement.getQuery();
            if (optimizer == null) {
                optimizer = new GSOptimizer(space);
                query = optimizer.validate(query);
            }
            RelNode logicalPlan = optimizer.createLogicalPlan(query);
            GSRelNode physicalPlan = optimizer.createPhysicalPlan(logicalPlan);
            ResponsePacket packet = handler.executeStatement(space, physicalPlan, params);
            return new PortalImpl<>(name, statement, Portal.Tag.SELECT, formatCodes, new ArrayIterator<>(packet.getResultEntry().getFieldValues()));
        } catch (SQLException e) {
            throw new NonBreakingException("Failed to execute query", e);
        }
    }

    private class StatementImpl implements Statement {
        private final String name;
        private final SqlNode query;
        private final StatementDescription description;

        public StatementImpl(String name, SqlNode query, StatementDescription description) {
            this.name = name;
            this.query = query;
            this.description = description;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getQueryString() {
            return query.toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();
        }

        @Override
        public SqlNode getQuery() {
            return query;
        }

        @Override
        public StatementDescription getDescription() {
            return description;
        }

        @Override
        public void close() throws Exception {
            closeS(name);
        }
    }

    private class PortalImpl<T> implements Portal<T>{
        private final String name;
        private final Statement stmt;
        private final Tag tag;
        private final RowDescription description;

        private final Iterator<T> it;

        private int processed;

        public PortalImpl(String name, Statement stmt, Tag tag, int[] formatCodes, Iterator<T> it) {
            this.name = name;
            this.stmt = stmt;
            this.tag = tag;
            this.it = it;

            RowDescription desc = stmt.getDescription().getRowDescription();
            if (formatCodes.length == 0 || (formatCodes.length == 1 && formatCodes[0] == 0))
                description = desc;
            else {
                List<ColumnDescription> columns = desc.getColumns();
                List<ColumnDescription> newColumns = new ArrayList<>();
                for (int i = 0, rowDescColumnsSize = columns.size(); i < rowDescColumnsSize; i++) {
                    ColumnDescription c = columns.get(i);
                    newColumns.add(new ColumnDescription(
                            c.getName(),
                            c.getType(),
                            c.getTypeLen(),
                            c.getTypeModifier(),
                            formatCodes.length == 1 ? formatCodes[0] : formatCodes[i],
                            c.getTableId(),
                            c.getTableIndex()));
                }
                description = new RowDescription(newColumns);
            }
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Statement getStatement() {
            return stmt;
        }

        @Override
        public RowDescription getDescription() {
            return description;
        }

        @Override
        public Tag tag() {
            return tag;
        }

        @Override
        public int processed() {
            return processed;
        }

        @Override
        public boolean hasNext() {
            return it != null && it.hasNext();
        }

        @Override
        public T next() {
            if (it == null)
                throw new NoSuchElementException();

            T next = it.next();
            processed++;
            return next;
        }

        @Override
        public void close() throws Exception {
            closeP(name);
        }
    }
}
