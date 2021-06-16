package com.gigaspaces.sql.aggregatornode.netty.query;

import com.fasterxml.jackson.databind.util.ArrayIterator;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.ITransportPacket;
import com.gigaspaces.jdbc.CalciteQueryHandler;
import com.gigaspaces.jdbc.QueryHandler;
import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.utils.Constants;
import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.gigaspaces.utils.Pair;
import com.google.common.collect.Iterables;
import com.j_spaces.jdbc.ConnectionContext;
import com.j_spaces.jdbc.IQueryProcessor;
import com.j_spaces.jdbc.QueryProcessorFactory;
import com.j_spaces.jdbc.ResponsePacket;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class QueryProviderImpl implements QueryProvider{
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(QueryProviderImpl.class);

    private static final AtomicLong COUNTER = new AtomicLong();

    private final ISpaceProxy space;
    private final CalciteQueryHandler handler;

    private Map<String, Statement> statements = new HashMap<>();
    private Map<String, Portal<?>> portals = new HashMap<>();

    public QueryProviderImpl(ISpaceProxy space) {
        this.space = space;
        this.handler = new CalciteQueryHandler();
    }

    public void init() throws ProtocolException {

    }

    @Override
    public void prepare(String stmt, String qry, int[] paramTypes) throws ProtocolException {
        log("prepare request for statement [{}] and query [{}]", stmt, qry);
        if (stmt.isEmpty())
            statements.remove(stmt);
        else if (statements.containsKey(stmt))
            throw new NonBreakingException("26000", "duplicate statement name");

        try {
//            statements.put(stmt, prepareStatement(stmt, qry, paramTypes)); todo
            statements.put(stmt, prepareStatement0(stmt, qry, paramTypes));
        } catch (Exception e) {
            throw new NonBreakingException("42000",	"failed to prepare statement", e);
        }
    }

    @Override
    public List<String> prepareMultiline(String qry) throws ProtocolException {
        log("prepareMultiling request for query [{}]", qry);
        ArrayList<String> res = new ArrayList<>();
        String[] split = qry.split(";"); // todo replace with statements tokenizer
        try {
            for (String s : split) {
                String stmt = "##___generated" + COUNTER.getAndIncrement();
                prepare(stmt, s, new int[0]);
                res.add(stmt);
            }
        } catch (Exception e) {
            for (String stmt : res) {
                try {
                    closeS(stmt);
                } catch (ProtocolException ex) {
                    e.addSuppressed(ex);
                }
            }
            throw e;
        }
        return res;
    }

    @Override
    public void bind(String portal, String stmt, Object[] params, int[] formatCodes) throws ProtocolException {
        log("Bind request for portal [{}] and statement [{}]", portal, stmt);
        if (!statements.containsKey(stmt))
            throw new NonBreakingException("26000", "invalid statement name");

        if (portal.isEmpty())
            portals.remove(stmt);
        else if (portals.containsKey(portal))
            throw new NonBreakingException("34000", "duplicate cursor name");

        try {
//            portals.put(portal, preparePortal(portal, statements.get(stmt), params, formatCodes)); todo
            portals.put(portal, preparePortal0(portal, statements.get(stmt), params, formatCodes));
        } catch (Exception e) {
            throw new NonBreakingException("42000",	"failed to bind statement to a portal", e);
        }
    }

    @Override
    public StatementDescription describeS(String stmt) throws ProtocolException {
        log("describeS for statement [{}]" ,stmt);
        if (!statements.containsKey(stmt))
            throw new NonBreakingException("26000", "invalid statement name ["+stmt+"]");

        return statements.get(stmt).getDescription();
    }

    @Override
    public RowDescription describeP(String portal) throws ProtocolException {
        log("describeP for portal [{}]" ,portal);
        if (!portals.containsKey(portal))
            throw new NonBreakingException("34000", "invalid cursor name ["+portal+"]");

        return portals.get(portal).getDescription();
    }

    @Override
    public Portal<?> execute(String portal) throws ProtocolException {
        log("execute portal [{}]", portal);
        if (!portals.containsKey(portal))
            throw new NonBreakingException("34000", "invalid cursor name ["+portal+"]");

        return portals.get(portal);
    }

    @Override
    public void closeS(String name) throws ProtocolException {
        log("closeS [{}]", name);
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
        log("closeP [{}]", name);
        portals.remove(name);
    }

    @Override
    public void cancel(int pid, int secret) {
        log("cancel pid={}, secret={}", pid, secret);
        // todo
    }

    private Statement prepareStatement(String name, String query, int[] paramTypes) throws Exception {
        log("prepareStatement name=[{}], query=[{}]", name, query);
        Pair<RelDataType, RelDataType> types = handler.extractTypes(query, space);

        RelDataType params = types.getFirst();
        RelDataType rows = types.getSecond();

        List<ParameterDescription> params0 = new ArrayList<>();
        for (RelDataTypeField f : params.getFieldList()) {
            params0.add(new ParameterDescription(TypeUtils.pgType(f.getType())));
        }
        ParametersDescription paramDesc = new ParametersDescription(params0);

        List<ColumnDescription> rows0 = new ArrayList<>();
        for (RelDataTypeField f : rows.getFieldList()) {
            rows0.add(new ColumnDescription(f.getName(), TypeUtils.pgType(f.getType())));
        }
        RowDescription rowDesc = new RowDescription(rows0);

        return new StatementImpl(name, query, new StatementDescription(paramDesc, rowDesc));
    }

    private Portal<?> preparePortal(String name, Statement statement, Object[] params, int[] formatCodes) throws Exception {
        log("preparePortal name=[{}]", name);
        ResponsePacket packet = handler.handle(statement.getQuery(), space, params);
        return new PortalImpl<>(name, statement, Portal.Tag.SELECT, formatCodes, new ArrayIterator<>(packet.getResultEntry().getFieldValues()));
    }

    // todo
    private static final String EMPTY_QUERY = "";
    private static final String SHOW_TRANSACTION_ISOLATION = "show transaction_isolation";
    private static final String SHOW_MAX_IDENTIFIER_LENGTH = "show max_identifier_length";
    private static final String SELECT_LO = "select oid, typbasetype from pg_type where typname = 'lo'";
    private static final String SELECT_NULL_NULL_NULL = "select NULL, NULL, NULL";
    private static final String SELECT_TABLES = "select relname, nspname, relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where relkind in ('r', 'v', 'm', 'f', 'p') and nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1') and n.oid = relnamespace order by nspname, relname";

    private Statement prepareStatement0(String name, String query, int[] paramTypes) throws Exception {
        log("prepareStatement0 name=[{}], query=[{}]", name, query);
        switch (query) {
            case SELECT_NULL_NULL_NULL: {
                ParametersDescription params = new ParametersDescription(paramTypes);
                ColumnDescription column1 = new ColumnDescription("col1", Constants.PG_TYPE_UNKNOWN);
                ColumnDescription column2 = new ColumnDescription("col2", Constants.PG_TYPE_UNKNOWN);
                ColumnDescription column3 = new ColumnDescription("col1", Constants.PG_TYPE_UNKNOWN);
                RowDescription rows = new RowDescription(Arrays.asList(column1, column2, column3));
                return new StatementImpl(name, query, new StatementDescription(params, rows));
            }
            case SELECT_LO: {
                ParametersDescription params = new ParametersDescription(paramTypes);
                ColumnDescription column1 = new ColumnDescription("oid", Constants.PG_TYPE_INT4, 4, -1);
                ColumnDescription column2 = new ColumnDescription("typbasetype", Constants.PG_TYPE_INT4, 4, -1);
                RowDescription rows = new RowDescription(Arrays.asList(column1, column2));
                return new StatementImpl(name, query, new StatementDescription(params, rows));
            }

            case SELECT_TABLES: {
                ParametersDescription params = new ParametersDescription(paramTypes);
                ColumnDescription column1 = new ColumnDescription("relname", Constants.PG_TYPE_VARCHAR);
                ColumnDescription column2 = new ColumnDescription("nspname", Constants.PG_TYPE_VARCHAR);
                ColumnDescription column3 = new ColumnDescription("relkind", Constants.PG_TYPE_CHAR, 1, -1);

                RowDescription rows = new RowDescription(Arrays.asList(column1, column2, column3));
                return new StatementImpl(name, query, new StatementDescription(params, rows));
            }

            case SHOW_TRANSACTION_ISOLATION: {
                ParametersDescription params = new ParametersDescription(paramTypes);
                ColumnDescription column = new ColumnDescription("transaction_isolation", Constants.PG_TYPE_VARCHAR);
                RowDescription rows = new RowDescription(Collections.singletonList(column));
                return new StatementImpl(name, query, new StatementDescription(params, rows));
            }

            case SHOW_MAX_IDENTIFIER_LENGTH: {
                ParametersDescription params = new ParametersDescription(paramTypes);
                ColumnDescription column = new ColumnDescription("max_identifier_length", Constants.PG_TYPE_INT4, 4, -1);
                RowDescription rows = new RowDescription(Collections.singletonList(column));
                return new StatementImpl(name, query, new StatementDescription(params, rows));
            }

            case EMPTY_QUERY: {
                ParametersDescription params = new ParametersDescription(paramTypes);
                RowDescription rows = new RowDescription(Collections.emptyList());
                return new StatementImpl(name, query, new StatementDescription(params, rows));
            }

            default:
                if (query.toLowerCase().startsWith("set ")) {
                    ParametersDescription params = new ParametersDescription(paramTypes);
                    RowDescription rows = new RowDescription(Collections.emptyList());
                    return new StatementImpl(name, query, new StatementDescription(params, rows));
                }
        }

        return prepareStatement(name, query, paramTypes);
    }

    private Portal<?> preparePortal0(String name, Statement statement, Object[] params, int[] formatCodes) throws Exception {
        log("preparePortal0 name=[{}]", name);
        switch (statement.getQuery()) {
            case SHOW_TRANSACTION_ISOLATION:
                return new PortalImpl<>(name, statement, Portal.Tag.SELECT, formatCodes, Collections.singletonList(
                        new Object[] {"SERIALIZABLE"}
                ).iterator());

            case SHOW_MAX_IDENTIFIER_LENGTH:
                return new PortalImpl<>(name, statement, Portal.Tag.SELECT, formatCodes, Collections.singletonList(
                        new Object[] {63}
                ).iterator());

            case SELECT_NULL_NULL_NULL:
                return new PortalImpl<>(name, statement, Portal.Tag.SELECT, formatCodes, Collections.singletonList(
                        new Object[] {null, null, null}
                ).iterator());

            case EMPTY_QUERY:
                return new PortalImpl<>(name, statement, Portal.Tag.NONE, formatCodes, Collections.emptyIterator());

            case SELECT_LO:
            case SELECT_TABLES:
                return new PortalImpl<>(name, statement, Portal.Tag.SELECT, formatCodes, Collections.emptyIterator());

            default:
                if (statement.getQuery().toLowerCase().startsWith("set "))
                    return new PortalImpl<>(name, statement, Portal.Tag.SELECT, formatCodes, Collections.emptyIterator());
        }

        return preparePortal(name, statement, params, formatCodes);
    }

    private class StatementImpl implements Statement {
        private final String name;
        private final String query;
        private final StatementDescription description;

        public StatementImpl(String name, String query, StatementDescription description) {
            this.name = name;
            this.query = query;
            this.description = description;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getQuery() {
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


    private void log(String str, Object...objects) {
        log.info("><QPI>< - " + str, objects);
    }
}
