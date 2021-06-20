package com.gigaspaces.sql.aggregatornode.netty.query;

import com.fasterxml.jackson.databind.util.ArrayIterator;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.jdbc.QueryHandler;
import com.gigaspaces.jdbc.calcite.GSOptimizer;
import com.gigaspaces.jdbc.calcite.GSRelNode;
import com.gigaspaces.jdbc.calcite.sql.extension.SqlShowOption;
import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.utils.ErrorCodes;
import com.gigaspaces.sql.aggregatornode.netty.utils.PgType;
import com.j_spaces.jdbc.ResponsePacket;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@SuppressWarnings({"unchecked", "rawtypes"})
public class QueryProviderImpl implements QueryProvider {
    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    private static final int[] EMPTY_INT_ARRAY = new int[0];
    private static final String EMPTY_STRING = "";

    private final ISpaceProxy space;
    private final QueryHandler handler;

    private final Map<String, Statement> statements = new HashMap<>();
    private final Map<String, Portal<?>> portals = new HashMap<>();

    public QueryProviderImpl(ISpaceProxy space) {
        this.space = space;
        this.handler = new QueryHandler();
    }

    public void init() {

    }

    @Override
    public void prepare(Session session, String stmt, String qry, int[] paramTypes) throws ProtocolException {
        if (stmt.isEmpty())
            statements.remove(stmt);
        else if (statements.containsKey(stmt))
            throw new NonBreakingException(ErrorCodes.INVALID_STATEMENT_NAME, "Duplicate statement name");

        try {
            statements.put(stmt, prepareStatement(stmt, qry, paramTypes));
        } catch (ProtocolException e) {
            throw e;
        } catch (Exception e) {
            throw new NonBreakingException(ErrorCodes.SYNTAX_ERROR,	"Failed to prepare statement", e);
        }
    }


    @Override
    public void bind(Session session, String portal, String stmt, Object[] params, int[] formatCodes) throws ProtocolException {
        if (!statements.containsKey(stmt))
            throw new NonBreakingException(ErrorCodes.INVALID_STATEMENT_NAME, "Invalid statement name");

        if (portal.isEmpty())
            portals.remove(stmt);
        else if (portals.containsKey(portal))
            throw new NonBreakingException(ErrorCodes.INVALID_CURSOR_NAME, "Duplicate cursor name");

        try {
            portals.put(portal, preparePortal(session, portal, statements.get(stmt), new GSOptimizer(space), params, formatCodes));
        } catch (ProtocolException e) {
            throw e;
        } catch (Exception e) {
            throw new NonBreakingException(ErrorCodes.INTERNAL_ERROR, "Failed to bind statement to a portal", e);
        }
    }

    @Override
    public StatementDescription describeS(String stmt) throws ProtocolException {
        if (!statements.containsKey(stmt))
            throw new NonBreakingException(ErrorCodes.INVALID_STATEMENT_NAME, "Invalid statement name");

        return statements.get(stmt).getDescription();
    }

    @Override
    public RowDescription describeP(String portal) throws ProtocolException {
        if (!portals.containsKey(portal))
            throw new NonBreakingException(ErrorCodes.INVALID_CURSOR_NAME, "Invalid cursor name");

        return portals.get(portal).getDescription();
    }

    @Override
    public Portal<?> execute(String portal) throws ProtocolException {
        if (!portals.containsKey(portal))
            throw new NonBreakingException(ErrorCodes.INVALID_CURSOR_NAME, "Invalid cursor name");

        Portal<?> res = portals.get(portal);
        res.execute();
        return res;
    }

    @Override
    public void closeS(String name) {
        List<String> toClose = portals.values().stream()
                .filter(p -> p.getStatement().getName().equals(name))
                .map(Portal::name)
                .collect(Collectors.toList());

        for (String portal : toClose)
            closeP(portal);

        statements.remove(name);
    }

    @Override
    public void closeP(String name) {
        portals.remove(name);
    }

    @Override
    public void cancel(int pid, int secret) {
        // TODO implement query cancel protocol
    }

    @Override
    public List<Portal<?>> executeQueryMultiline(Session session, String query) throws ProtocolException {
        try {
            if (query.trim().isEmpty()) {
                ParametersDescription paramDesc = new ParametersDescription();
                RowDescription rowDesc = new RowDescription();
                StatementDescription description = new StatementDescription(paramDesc, rowDesc);
                StatementImpl statement = new StatementImpl(EMPTY_STRING, null, description);
                EmptyPortal<Object> portal = new EmptyPortal<>(EMPTY_STRING, statement);
                return Collections.singletonList(portal);
            }

            GSOptimizer optimizer = new GSOptimizer(space);
            SqlNodeList nodes = optimizer.parseMultiline(query);
            List<Portal<?>> result = new ArrayList<>();
            for (SqlNode node : nodes) {
                StatementDescription description = describe(optimizer, node, EMPTY_INT_ARRAY);
                StatementImpl statement = new StatementImpl(EMPTY_STRING, node, description);
                result.add(preparePortal(session, EMPTY_STRING, statement, optimizer, EMPTY_OBJECT_ARRAY, EMPTY_INT_ARRAY));
            }
            return result;
        } catch (ProtocolException e) {
            throw e;
        } catch (Exception e) {
            throw new NonBreakingException(ErrorCodes.INTERNAL_ERROR, "Failed to execute query", e);
        }
    }

    private StatementImpl prepareStatement(String name, String query, int[] paramTypes) throws ProtocolException {
        if (query.trim().isEmpty()) {
            assert paramTypes.length == 0;
            ParametersDescription paramDesc = new ParametersDescription();
            RowDescription rowDesc = new RowDescription();
            return new StatementImpl(name, null, new StatementDescription(paramDesc, rowDesc));
        }

        GSOptimizer optimizer = new GSOptimizer(space);
        SqlNode sqlNode = optimizer.parse(query);
        StatementDescription description = describe(optimizer, sqlNode, paramTypes);
        return new StatementImpl(name, sqlNode, description);
    }

    private StatementDescription describe(GSOptimizer optimizer, SqlNode queryAst, int[] paramTypes) {
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

        RelDataType columns = null;
        if (queryAst.isA(SqlKind.FUNCTION)) {
            if (queryAst.getKind() == SqlKind.OTHER_FUNCTION) {
                SqlCall call = (SqlCall) queryAst;
                if (call.getOperator() == SqlShowOption.OPERATOR) {
                    SqlShowOption option = (SqlShowOption) call;
                    String var = option.getName().toString();
                    RelDataTypeFactory tf = optimizer.typeFactory();
                    switch (var.toLowerCase(Locale.ROOT)) {
                        case "transaction_isolation":
                        case "client_encoding":
                        case "datestyle": {
                            columns = tf.createStructType(
                                    singletonList(tf.createJavaType(String.class)),
                                    singletonList("COL1"));
                            break;
                        }
                        case "statement_timeout":
                        case "extra_float_digits": {
                            columns = tf.createStructType(
                                    singletonList(tf.createJavaType(Integer.class)),
                                    singletonList("COL1"));
                            break;
                        }

                        default: {
                            columns = tf.createStructType(
                                    singletonList(tf.createUnknownType()),
                                    singletonList("COL1"));
                        }
                    }
                }
            }
        }
        if (queryAst.isA(SqlKind.DDL))
            columns = optimizer.typeFactory().createStructType(emptyList());

        if (columns == null)
            columns = optimizer.extractRowType(optimizer.validate(queryAst));

        List<ColumnDescription> columns0 = new ArrayList<>();
        for (RelDataTypeField f : columns.getFieldList()) {
            columns0.add(new ColumnDescription(f.getName(), PgType.fromInternal(f.getType())));
        }
        RowDescription rowDesc = new RowDescription(columns0);
        return new StatementDescription(paramDesc, rowDesc);
    }

    private Portal<?> preparePortal(Session session, String name, Statement statement, GSOptimizer optimizer, Object[] params, int[] formatCodes) throws ProtocolException {
        SqlNode query = statement.getQuery();
        if (query == null)
            return new EmptyPortal<>(name, statement);

        if (query.isA(SqlKind.DDL)) {
            return prepareDdl(session, name, statement, query);
        }

        if (query.isA(SqlKind.QUERY)) {
            return prepareQuery(name, statement, optimizer, params, formatCodes, query);
        }

        if (query.isA(SqlKind.FUNCTION)) {
            return prepareFunction(session, name, statement, optimizer, params, formatCodes, query);
        }

        throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Unsupported query kind: " + query.getKind());
    }

    private Portal<?> prepareFunction(Session session, String name, Statement statement, GSOptimizer optimizer, Object[] params, int[] formatCodes, SqlNode query) throws ProtocolException {
        if (query.getKind() == SqlKind.OTHER_FUNCTION) {
            SqlCall call = (SqlCall) query;
            if (call.getOperator() == SqlShowOption.OPERATOR) {
                SqlShowOption option = (SqlShowOption) call;
                String var = option.getName().toString();
                switch (var.toLowerCase(Locale.ROOT)) {
                    case "transaction_isolation":
                        return new QueryPortal(name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{"READ_COMMITTED"}).iterator());
                    case "client_encoding":
                        return new QueryPortal(name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{session.getCharset().name()}).iterator());
                    case "datestyle":
                        return new QueryPortal(name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{session.getDateStyle()}).iterator());
                    case "statement_timeout":
                        return new QueryPortal(name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{0}).iterator());
                    case "extra_float_digits":
                        return new QueryPortal(name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{2}).iterator());
                    default:
                        return new QueryPortal(name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, Collections::emptyIterator);
                }
            }
        }
        
        return prepareQuery(name, statement, optimizer, params, formatCodes, query);
    }

    private Portal<?> prepareDdl(Session session, String name, Statement statement, SqlNode query) throws ProtocolException {
        if (query.getKind() == SqlKind.SET_OPTION) {
            SqlSetOption setOption = (SqlSetOption) query;
            String var = setOption.getName().toString();

            if (!SqlUtil.isLiteral(setOption.getValue()))
                throw new NonBreakingException(ErrorCodes.INVALID_PARAMETER_VALUE,
                        setOption.getValue().getParserPosition(), "Literal value is expected.");

            SqlLiteral literal = (SqlLiteral) setOption.getValue();
            switch (var.toLowerCase(Locale.ROOT)) {
                case "client_encoding": {
                    String val = asString(literal);
                    ThrowingSupplier<Integer, ProtocolException> op = () -> {
                        try {
                            session.setCharset(Charset.forName(val));
                        } catch (Exception e) {
                            throw new NonBreakingException(ErrorCodes.INVALID_PARAMETER_VALUE, literal.getParserPosition(), "Unknown charset");
                        }
                        return 1;
                    };

                    return new DmlPortal<>(name, statement, PortalCommand.SET, op);
                }

                case "datestyle": {
                    String val = asString(literal);
                    ThrowingSupplier<Integer, ProtocolException> op = () -> {
                        session.setDateStyle(val.indexOf(',') < 0 ? val + ", MDY" : val);
                        return 1;
                    };

                    return new DmlPortal<>(name, statement, PortalCommand.SET, op);
                }

                default:
                    // TODO support missing variables
                    return new DmlPortal<>(name, statement, PortalCommand.SET, () -> 0);
            }
        }

        throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Unsupported query kind: " + query.getKind());
    }

    private String asString(SqlLiteral literal) throws NonBreakingException {
        if (!SqlLiteral.valueMatchesType(literal.getValue(), SqlTypeName.CHAR))
            throw new NonBreakingException(ErrorCodes.INVALID_PARAMETER_VALUE,
                    literal.getParserPosition(), "String literal is expected.");
        return literal.getValueAs(String.class);
    }

    private Portal<?> prepareQuery(String name, Statement statement, GSOptimizer optimizer, Object[] params, int[] formatCodes, SqlNode query) throws ProtocolException {
        try {
            query = optimizer.validate(query);
            RelNode logicalPlan = optimizer.createLogicalPlan(query);
            GSRelNode physicalPlan = optimizer.createPhysicalPlan(logicalPlan);
            ThrowingSupplier<Iterator<Object[]>, ProtocolException> op = () -> {
                try {
                    ResponsePacket packet = handler.executeStatement(space, physicalPlan, params);
                    return new ArrayIterator<>(packet.getResultEntry().getFieldValues());
                } catch (SQLException e) {
                    throw new NonBreakingException(ErrorCodes.INTERNAL_ERROR, "Failed to execute operation.", e);
                }
            };
            return new QueryPortal<>(name, statement, PortalCommand.SELECT, formatCodes, op);
        } catch (Exception e) {
            throw new NonBreakingException(ErrorCodes.INTERNAL_ERROR, "Failed to prepare portal", e);
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
        public SqlNode getQuery() {
            return query;
        }

        @Override
        public StatementDescription getDescription() {
            return description;
        }

        @Override
        public void close() {
            closeS(name);
        }
    }

    private enum PortalCommand {
        SHOW("SHOW"),
        SELECT("SELECT"),
        DELETE("DELETE"),
        UPDATE("UPDATE"),
        INSERT("INSERT"),
        SET("SET");

        private final String tag;

        PortalCommand(String tag) {
            this.tag = tag;
        }

        public String tag() {
            return tag;
        }
    }

    private class EmptyPortal<T> implements Portal<T> {
        private final String name;
        private final Statement statement;

        private EmptyPortal(String name, Statement statement) {
            this.name = name;
            this.statement = statement;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Statement getStatement() {
            return statement;
        }

        @Override
        public void close() {
            closeP(name);
        }

        @Override
        public boolean empty() {
            return true;
        }

        @Override
        public RowDescription getDescription() {
            return new RowDescription();
        }

        @Override
        public String tag() {
            return EMPTY_STRING;
        }

        @Override
        public void execute() {
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }
    }

    private class QueryPortal<T> implements Portal<T>{
        private final String name;
        private final Statement stmt;
        private final RowDescription description;
        private final PortalCommand command;
        private final ThrowingSupplier<Iterator<T>, ProtocolException> op;

        private int processed;
        private Iterator<T> it;

        public QueryPortal(String name, Statement stmt, PortalCommand command, int[] formatCodes, ThrowingSupplier<Iterator<T>, ProtocolException> op) {
            this.name = name;
            this.stmt = stmt;
            this.command = command;
            this.op = op;

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
        public String tag() {
            if (command == PortalCommand.SELECT)
                return String.format("%s 0 %d", command.tag(), processed);
            return String.format("%s %d", command.tag(), processed);
        }

        @Override
        public void execute() throws ProtocolException {
            if (it != null)
                throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Duplicate execute message");
            it = op.apply();
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
        public void close() {
            closeP(name);
        }
    }

    private class DmlPortal<T> implements Portal<T> {
        private final String name;
        private final Statement statement;
        private final PortalCommand command;
        private final ThrowingSupplier<Integer, ProtocolException> op;

        private Integer processed;

        private DmlPortal(String name, Statement statement, PortalCommand command, ThrowingSupplier<Integer, ProtocolException> op) {
            this.name = name;
            this.statement = statement;
            this.command = command;
            this.op = op;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Statement getStatement() {
            return statement;
        }

        @Override
        public RowDescription getDescription() {
            return new RowDescription();
        }

        @Override
        public String tag() {
            switch (command) {
                case DELETE:
                case UPDATE:
                case INSERT:
                    return String.format("%s 0 %d", command.tag(), processed);
                default:
                    return String.format("%s %d", command.tag(), processed);
            }
        }

        @Override
        public void execute() throws ProtocolException {
            processed = op.apply();
        }

        @Override
        public void close() {
            closeP(name);
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }
    }
}
