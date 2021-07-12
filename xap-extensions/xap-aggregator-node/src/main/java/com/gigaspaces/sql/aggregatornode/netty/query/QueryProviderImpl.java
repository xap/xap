/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.sql.aggregatornode.netty.query;

import com.fasterxml.jackson.databind.util.ArrayIterator;
import com.gigaspaces.jdbc.calcite.GSOptimizer;
import com.gigaspaces.jdbc.calcite.GSOptimizerValidationResult;
import com.gigaspaces.jdbc.calcite.GSRelNode;
import com.gigaspaces.jdbc.calcite.handlers.CalciteQueryHandler;
import com.gigaspaces.jdbc.calcite.sql.extension.SqlShowOption;
import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ParseException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.utils.*;
import com.j_spaces.jdbc.ResponsePacket;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.gigaspaces.sql.aggregatornode.netty.utils.Constants.EMPTY_INT_ARRAY;
import static com.gigaspaces.sql.aggregatornode.netty.utils.DateTimeUtils.convertTimeZone;
import static java.util.Collections.singletonList;

@SuppressWarnings({"unchecked", "rawtypes"})
public class QueryProviderImpl implements QueryProvider {

    private static final int DML_SINGLE_VALUE_MODIFIED = 1;

    private final CalciteQueryHandler handler;

    private final Map<String, Statement> statements = new HashMap<>();
    private final Map<String, Portal<?>> portals = new HashMap<>();

    public QueryProviderImpl() {
        this.handler = new CalciteQueryHandler();
    }

    @Override
    public void prepare(Session session, String stmt, String qry, int[] paramTypes) throws ProtocolException {
        if (stmt.isEmpty())
            statements.remove(stmt);
        else if (statements.containsKey(stmt))
            throw new NonBreakingException(ErrorCodes.INVALID_STATEMENT_NAME, "Duplicate statement name");

        try {
            statements.put(stmt, prepareStatement(session, stmt, qry, paramTypes));
        } catch (Exception e) {
            throw new NonBreakingException(ErrorCodes.SYNTAX_ERROR, "Failed to prepare statement", e);
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
            portals.put(portal, preparePortal(session, portal, statements.get(stmt), params, formatCodes));
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
            // TODO possibly it's worth to add SqlEmptyNode to sql parser
            if (query.trim().isEmpty()) {
                StatementImpl statement = new StatementImpl(this, Constants.EMPTY_STRING, null, null, StatementDescription.EMPTY);
                EmptyPortal<Object> portal = new EmptyPortal<>(this, Constants.EMPTY_STRING, statement);
                return Collections.singletonList(portal);
            }

            GSOptimizer optimizer = new GSOptimizer(session.getSpace());

            SqlNodeList nodes = optimizer.parseMultiline(query);

            List<Portal<?>> result = new ArrayList<>();
            for (SqlNode node : nodes) {
                StatementImpl statement = prepareStatement(session, Constants.EMPTY_STRING, optimizer, EMPTY_INT_ARRAY, node);
                result.add(preparePortal(session, Constants.EMPTY_STRING, statement, Constants.EMPTY_OBJECT_ARRAY, EMPTY_INT_ARRAY));
            }
            return result;
        } catch (ProtocolException e) {
            throw e;
        } catch (Exception e) {
            throw new NonBreakingException(ErrorCodes.INTERNAL_ERROR, "Failed to execute query", e);
        }
    }

    private StatementImpl prepareStatement(Session session, String name, String query, int[] paramTypes) throws ParseException {
        // TODO possibly it's worth to add SqlEmptyNode to sql parser
        if (query.trim().isEmpty()) {
            assert paramTypes.length == 0;
            return new StatementImpl(this, name, null, null, StatementDescription.EMPTY);
        }
        GSOptimizer optimizer = new GSOptimizer(session.getSpace());
        try {
            return prepareStatement(session, name, optimizer, paramTypes, optimizer.parse(query));
        } catch (SqlParseException e) {
            throw new ParseException(e.getMessage(), e);
        }
    }

    private StatementImpl prepareStatement(Session session, String name, GSOptimizer optimizer, int[] paramTypes, SqlNode ast) {
        if (SqlUtil.isCallTo(ast, SqlShowOption.OPERATOR)) {
            StatementDescription description = describeShow((SqlShowOption) ast);
            return new StatementImpl(this, Constants.EMPTY_STRING, ast, optimizer, description);
        } else if (ast.getKind() == SqlKind.SET_OPTION) {
            // all parameters should be literals
            return new StatementImpl(this, Constants.EMPTY_STRING, ast, optimizer, StatementDescription.EMPTY);
        } else {
            GSOptimizerValidationResult validated = optimizer.validate(ast);

            ParametersDescription paramDesc;
            if (paramTypes.length > 0) {
                paramDesc = new ParametersDescription(paramTypes);
            } else {
                RelDataType paramType = validated.getParameterRowType();
                List<ParameterDescription> params = new ArrayList<>(paramType.getFieldCount());
                for (RelDataTypeField field : paramType.getFieldList()) {
                    params.add(new ParameterDescription(TypeUtils.fromInternal(field.getType())));
                }
                paramDesc = new ParametersDescription(params);
            }

            RelDataType rowType = validated.getRowType();
            List<ColumnDescription> columns = new ArrayList<>(rowType.getFieldCount());
            for (RelDataTypeField field : rowType.getFieldList()) {
                columns.add(new ColumnDescription(field.getName(), TypeUtils.fromInternal(field.getType())));
            }
            RowDescription rowDesc = new RowDescription(columns);

            StatementDescription description = new StatementDescription(paramDesc, rowDesc);
            return new StatementImpl(this, name, validated.getValidatedAst(), optimizer, description);
        }
    }

    private StatementDescription describeShow(SqlShowOption node) {
        PgType type;

        String name = node.getName().toString();
        switch (name.toLowerCase(Locale.ROOT)) {
            case "transaction_isolation":
            case "client_encoding":
            case "datestyle": {
                type = TypeVarchar.INSTANCE;
                break;
            }
            case "max_identifier_length":
            case "statement_timeout":
            case "extra_float_digits": {
                type = TypeInt4.INSTANCE;
                break;
            }

            default: {
                type = TypeUnknown.INSTANCE;
                break;
            }
        }

        return new StatementDescription(ParametersDescription.EMPTY,
                new RowDescription(singletonList(new ColumnDescription(name, type))));
    }

    private Portal<?> preparePortal(Session session, String name, Statement statement, Object[] params, int[] formatCodes) throws ProtocolException {
        SqlNode query = statement.getQuery();

        if (query == null)
            return new EmptyPortal<>(this, name, statement);

        if (query.getKind() == SqlKind.SET_OPTION) {
            return prepareSetOption(session, name, statement, (SqlSetOption) query);
        }

        if (query.isA(SqlKind.QUERY)) {
            return prepareQuery(session, name, statement, params, formatCodes, query);
        }

        if (SqlUtil.isCallTo(query, SqlShowOption.OPERATOR)) {
            return prepareShowOption(session, name, statement, (SqlShowOption) query);
        }

        throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Unsupported query kind: " + query.getKind());
    }

    private Portal<?> prepareShowOption(Session session, String name, Statement statement, SqlShowOption show) {
        String var = show.getName().toString();
        switch (var.toLowerCase(Locale.ROOT)) {
            case "transaction_isolation":
                return new QueryPortal(this, name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{"READ_COMMITTED"}).iterator());
            case "client_encoding":
                return new QueryPortal(this, name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{session.getCharset().name()}).iterator());
            case "datestyle":
                return new QueryPortal(this, name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{session.getDateStyle()}).iterator());
            case "statement_timeout":
                return new QueryPortal(this, name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{0}).iterator());
            case "extra_float_digits":
                return new QueryPortal(this, name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{2}).iterator());
            case "max_identifier_length":
                return new QueryPortal(this, name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, () -> singletonList(new Object[]{63}).iterator());
            default:
                return new QueryPortal(this, name, statement, PortalCommand.SHOW, EMPTY_INT_ARRAY, Collections::emptyIterator);
        }
    }

    private Portal<?> prepareSetOption(Session session, String name, Statement statement, SqlSetOption query) throws NonBreakingException {
        String var = query.getName().toString();

        if (!SqlUtil.isLiteral(query.getValue()))
            throw new NonBreakingException(ErrorCodes.INVALID_PARAMETER_VALUE,
                    query.getValue().getParserPosition(), "Literal value is expected.");

        SqlLiteral literal = (SqlLiteral) query.getValue();
        switch (var.toLowerCase(Locale.ROOT)) {
            case "client_encoding": {
                String val = asString(literal);
                ThrowingSupplier<Integer, ProtocolException> op = () -> {
                    try {
                        session.setCharset(Charset.forName(val));
                    } catch (Exception e) {
                        throw new NonBreakingException(ErrorCodes.INVALID_PARAMETER_VALUE, literal.getParserPosition(), "Unknown charset");
                    }
                    return DML_SINGLE_VALUE_MODIFIED;
                };

                return new DmlPortal<>(this, name, statement, PortalCommand.SET, op);
            }

            case "datestyle": {
                String val = asString(literal);
                ThrowingSupplier<Integer, ProtocolException> op = () -> {
                    session.setDateStyle(val.indexOf(',') < 0 ? val + ", MDY" : val);
                    return DML_SINGLE_VALUE_MODIFIED;
                };

                return new DmlPortal<>(this, name, statement, PortalCommand.SET, op);
            }

            case "timezone" : {
                String val = asString(literal);
                ThrowingSupplier<Integer, ProtocolException> op = () -> {
                    session.setTimeZone(TimeZone.getTimeZone(convertTimeZone(val)));
                    return DML_SINGLE_VALUE_MODIFIED;
                };

                return new DmlPortal<>(this, name, statement, PortalCommand.SET, op);
            }

            default:
                // TODO support missing variables
                return new DmlPortal<>(this, name, statement, PortalCommand.SET, () -> 0);
        }
    }

    private Portal<?> prepareQuery(Session session, String name, Statement statement, Object[] params, int[] formatCodes, SqlNode query) throws ProtocolException {
        try {
            ThrowingSupplier<Iterator<Object[]>, ProtocolException> op = () -> {
                try {
                    GSRelNode physicalPlan = statement.getOptimizer().optimize(query);
                    ResponsePacket packet = handler.executeStatement(session.getSpace(), physicalPlan, params);
                    return new ArrayIterator<>(packet.getResultEntry().getFieldValues());
                } catch (SQLException e) {
                    throw new NonBreakingException(ErrorCodes.INTERNAL_ERROR, "Failed to execute operation.", e);
                }
            };
            return new QueryPortal<>(this, name, statement, PortalCommand.SELECT, formatCodes, op);
        } catch (Exception e) {
            throw new NonBreakingException(ErrorCodes.INTERNAL_ERROR, "Failed to prepare portal", e);
        }
    }

    private String asString(SqlLiteral literal) throws NonBreakingException {
        if (!SqlLiteral.valueMatchesType(literal.getValue(), SqlTypeName.CHAR))
            throw new NonBreakingException(ErrorCodes.INVALID_PARAMETER_VALUE,
                    literal.getParserPosition(), "String literal is expected.");
        return literal.getValueAs(String.class);
    }
}
