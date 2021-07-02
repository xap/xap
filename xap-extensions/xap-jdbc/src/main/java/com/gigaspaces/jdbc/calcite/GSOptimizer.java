package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.calcite.parser.GSSqlParserFactoryWrapper;
import com.gigaspaces.jdbc.calcite.sql.extension.GSSqlOperatorTable;
import com.gigaspaces.jdbc.calcite.pg.PgCalciteSchema;
import com.j_spaces.core.IJSpace;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.calcite.sql.validate.SqlConformanceEnum.LENIENT;

public class GSOptimizer {

    public static final String ROOT_SCHEMA_NAME = "root";

    private static final CalciteConnectionConfig CONNECTION_CONFIG = CalciteConnectionConfig.DEFAULT
        .set(CalciteConnectionProperty.PARSER_FACTORY, GSSqlParserFactoryWrapper.FACTORY_CLASS)
        .set(CalciteConnectionProperty.CASE_SENSITIVE, "false")
        .set(CalciteConnectionProperty.QUOTED_CASING, Casing.UNCHANGED.toString())
        .set(CalciteConnectionProperty.UNQUOTED_CASING, Casing.UNCHANGED.toString());

    private static final SqlParser.Config PARSER_CONFIG = SqlParser.configBuilder()
        .setParserFactory(GSSqlParserFactoryWrapper.FACTORY)
        .setQuotedCasing(Casing.UNCHANGED)
        .setUnquotedCasing(Casing.UNCHANGED)
        .setCaseSensitive(false).build();

    private final CalciteCatalogReader catalogReader;
    private final SqlValidator validator;
    private final VolcanoPlanner planner;
    private final RelOptCluster cluster;

    public GSOptimizer(IJSpace space) {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();

        catalogReader = new GSCalciteCatalogReader(
            createSchema(space),
            Arrays.asList(
                Collections.singletonList(ROOT_SCHEMA_NAME),
                Collections.singletonList(PgCalciteSchema.NAME),
                Collections.emptyList()
            ),
            typeFactory,
            CONNECTION_CONFIG);

        validator = SqlValidatorUtil.newValidator(
                ChainedSqlOperatorTable.of(
                        GSSqlOperatorTable.instance()
                        , SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(SqlLibrary.STANDARD, SqlLibrary.POSTGRESQL)
                ),
            catalogReader, typeFactory,
            SqlValidator.Config.DEFAULT.withSqlConformance( LENIENT ));

        planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(CONNECTION_CONFIG));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.setTopDownOpt(true);

        cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    public SqlNode parse(String query) throws SqlParseException {
        SqlParseException ex = null;
        while (true) {
            try {
                return createParser(query).parseQuery();
            } catch (SqlParseException e) {
                if (ex != null)
                    throw ex;
                try {
                    query = encloseParentheses(query);
                } catch (Exception e1) {
                    throw e;
                }
                ex = e;
            }
        }
    }

    public SqlNodeList parseMultiline(String query) throws SqlParseException {
        SqlParseException ex = null;
        while (true) {
            try {
                return createParser(query).parseStmtList();
            } catch (SqlParseException e) {
                if (ex != null)
                    throw ex;
                try {
                    query = encloseParentheses(query);
                } catch (Exception e1) {
                    throw e;
                }
                ex = e;
            }
        }
    }

    private SqlParser createParser(String query) {
        return SqlParser.create(query, PARSER_CONFIG);
    }

    public GSOptimizerValidationResult validate(SqlNode ast) {
        SqlNode validatedAst = validator.validate(ast);
        RelDataType rowType = validator.getValidatedNodeType(validatedAst);
        RelDataType parameterRowType = validator.getParameterRowType(validatedAst);

        return new GSOptimizerValidationResult(validatedAst, rowType, parameterRowType);
    }

    public GSRelNode optimize(SqlNode validatedAst) {
        RelNode logicalPlan = optimizeLogical(validatedAst, this.validator);
        return optimizePhysical(logicalPlan);
    }

    private RelNode optimizeLogical(SqlNode validatedAst, SqlValidator validator) {
        SqlToRelConverter relConverter = new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(true)
                .withExpand(false)
                .build());

        // TODO: Careful with RelRoot removal here - need to add the top-level project
        return relConverter.convertQuery(validatedAst, false, true).rel;
    }

    private GSRelNode optimizePhysical(RelNode logicalPlan) {
        Program program = GSOptimizerProgram.createProgram();

        RelNode res = program.run(
            planner,
            logicalPlan,
            logicalPlan.getTraitSet().plus(GSConvention.INSTANCE),
            Collections.emptyList(),
            Collections.emptyList()
        );

        return (GSRelNode) res;
    }

    private static CalciteSchema createSchema(IJSpace space) {
        CalciteSchema res = CalciteSchema.createRootSchema(true, false);
        res.add(ROOT_SCHEMA_NAME, new GSSchema(space));
        res.add(PgCalciteSchema.NAME, PgCalciteSchema.INSTANCE);
        return res;
    }

    private String encloseParentheses(String src) {
        StringBuilder curr = new StringBuilder();
        StringBuilder temp = new StringBuilder();
        String lastToken = "";
        int offset = 0;
        while (offset < src.length()) {
            char c = src.charAt(offset);
            if (c == '(') {
                if (temp.length() > 0) {
                    lastToken = temp.toString();
                    temp.setLength(0);
                    separate(curr).append(lastToken);
                }
                offset = encloseParentheses(curr, src, offset + 1, lastToken);
                continue;
            } else if (c == ')')
                throw new RuntimeException("Unexpected symbol ')' at col " + offset);
            else if (Character.isWhitespace(c)) {
                if (temp.length() > 0) {
                    lastToken = temp.toString();
                    temp.setLength(0);
                    separate(curr).append(lastToken);
                }
                curr.append(c);
            } else {
                temp.append(c);
            }
            offset++;
        }

        if (temp.length() > 0)
            separate(curr).append(temp);
        return curr.toString();
    }

    private int encloseParentheses(StringBuilder dst, String src, int offset, String lastToken) {
        String lastToken0 = lastToken;
        String firstToken = "";
        StringBuilder curr = new StringBuilder();
        StringBuilder temp = new StringBuilder();
        while (offset < src.length()) {
            char c = src.charAt(offset);
            if (c == '(') {
                if (temp.length() > 0) {
                    lastToken0 = temp.toString();
                    temp.setLength(0);
                    if (firstToken.isEmpty())
                        firstToken = lastToken0;
                    separate(curr).append(lastToken0);
                }
                offset = encloseParentheses(curr, src, offset + 1, lastToken0);
                continue;
            } else if (c == ')') {
                if (temp.length() > 0) {
                    lastToken0 = temp.toString();
                    temp.setLength(0);
                    if (firstToken.isEmpty())
                        firstToken = lastToken0;
                    separate(curr).append(lastToken0);
                }
                if (lastToken.equalsIgnoreCase("from") && !firstToken.equalsIgnoreCase("select")) {
                    separate(dst).append(curr);
                } else {
                    dst.append('(').append(curr).append(')');
                }
                return offset + 1;
            } else if (Character.isWhitespace(c)) {
                if (temp.length() > 0) {
                    lastToken0 = temp.toString();
                    temp.setLength(0);
                    if (firstToken.isEmpty())
                        firstToken = lastToken0;
                    separate(curr).append(lastToken0);
                }
                curr.append(c);
            } else {
                temp.append(c);
            }
            offset++;
        }
        throw new RuntimeException("Unexpected end of string");
    }

    private StringBuilder separate(StringBuilder curr) {
        if (curr.length() == 0)
            return curr;
        if (Character.isWhitespace(curr.charAt(curr.length() - 1)))
            return curr;
        return curr.append(' ');
    }
}
