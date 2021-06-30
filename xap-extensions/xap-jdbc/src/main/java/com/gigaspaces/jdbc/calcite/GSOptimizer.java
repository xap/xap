package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.calcite.parser.GSSqlParserFactoryWrapper;
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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;

import java.util.Collections;

import static org.apache.calcite.sql.validate.SqlConformanceEnum.LENIENT;

public class GSOptimizer {
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

        catalogReader = new CalciteCatalogReader(
            createSchema(space),
            Collections.singletonList("root"),
            typeFactory,
            CONNECTION_CONFIG);

        validator = SqlValidatorUtil.newValidator(
            SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(SqlLibrary.STANDARD, SqlLibrary.POSTGRESQL, SqlLibrary.BIG_QUERY),
            catalogReader, typeFactory,
            SqlValidator.Config.DEFAULT.withSqlConformance( LENIENT ));

        planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(CONNECTION_CONFIG));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.setTopDownOpt(true);

        cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    public SqlNode parse(String query) throws SqlParseException {
        SqlParser parser = createParser(query);

        return parser.parseQuery();
    }

    public SqlNodeList parseMultiline(String query) {
        SqlParser parser = createParser(query);

        try {
            return parser.parseStmtList();
        } catch (SqlParseException e) {
            throw new RuntimeException("Failed to parse the query.", e);
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
        res.add("root", new GSSchema(space));
        res.add("pg_catalog", new GSSchema(space));
        return res;
    }
}
