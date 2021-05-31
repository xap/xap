package com.gigaspaces.jdbc.calcite;

import com.google.common.collect.ImmutableList;
import com.j_spaces.core.IJSpace;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
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
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GSOptimizer {

    private final IJSpace space;

    private final JavaTypeFactoryImpl typeFactory;
    private final CalciteCatalogReader catalogReader;
    private final SqlValidator validator;
    private final VolcanoPlanner planner;
    private final RelOptCluster cluster;

    public GSOptimizer(IJSpace space) {
        this.space = space;

        CalciteConnectionConfig config = CalciteConnectionConfig.DEFAULT
            .set(CalciteConnectionProperty.CASE_SENSITIVE, "false")
            .set(CalciteConnectionProperty.QUOTED_CASING, Casing.UNCHANGED.toString())
            .set(CalciteConnectionProperty.UNQUOTED_CASING, Casing.UNCHANGED.toString());

        typeFactory = new JavaTypeFactoryImpl();

        catalogReader = new CalciteCatalogReader(
            createSchema(space),
            Collections.singletonList("root"),
            typeFactory,
            config);

        validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader, typeFactory,
            SqlValidator.Config.DEFAULT.withDefaultNullCollation(NullCollation.FIRST));

        planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.setTopDownOpt(true);

        cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    public SqlNode parse(String query) throws SqlParseException {
        SqlParser.Config config = SqlParser.configBuilder()
            .setQuotedCasing(Casing.UNCHANGED)
            .setUnquotedCasing(Casing.UNCHANGED)
            .setCaseSensitive(false).build();

        SqlParser parser = SqlParser.create(query, config);
        return parser.parseQuery();
    }

    public SqlNode validate(SqlNode ast) {
        return validator.validate(ast);
    }

    public RelRoot createLogicalPlan(SqlNode validatedAst) {
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
        return relConverter.convertQuery(validatedAst, false, true);
    }

    public GSRelNode createPhysicalPlan(RelRoot logicalRoot) {
        Program program = GSOptimizerProgram.createProgram();
        RelNode logicalPlan = logicalRoot.rel;
        RelNode res = program.run(
                planner,
                logicalPlan,
                logicalPlan.getTraitSet().replace(logicalRoot.collation).replace(GSConvention.INSTANCE),
                Collections.emptyList(),
                Collections.emptyList()
        );

        // An additional projection is required if RelRoot.project() return an object
        // which is different from the top-level operator.
        boolean requiresProject = logicalRoot.project() != logicalRoot.rel;
        if (requiresProject) {
            // Create logical project to deduce the return type and Calc program.
            ImmutableList<Pair<Integer, String>> fields = logicalRoot.fields;
            List<RexNode> projects = new ArrayList<>(fields.size());
            RexBuilder rexBuilder = res.getCluster().getRexBuilder();
            for (Pair<Integer, String> field : fields) {
                projects.add(rexBuilder.makeInputRef(res, field.left));
            }
            LogicalProject project = LogicalProject.create(
                    res,
                    Collections.emptyList(),
                    projects,
                    Pair.right(fields)
            );

            // Create the Calc program, similarly to GSProjectToCalcRule.onMatch
            RexProgram calcProgram = RexProgram.create(
                    res.getRowType(),
                    project.getProjects(),
                    null,
                    project.getRowType(),
                    project.getCluster().getRexBuilder()
            );
            // Install the GSCalc on top of the optimized node.
            res = new GSCalc(
                    project.getCluster(),
                    res.getCluster().traitSet().replace(GSConvention.INSTANCE),
                    Collections.emptyList(),
                    res,
                    calcProgram
            );
        }
        return (GSRelNode) res;
    }

    private static CalciteSchema createSchema(IJSpace space) {
        CalciteSchema res = CalciteSchema.createRootSchema(true, false);
        res.add("root", new GSSchema(space));
        res.add("pg_catalog", new GSSchema(space));
        return res;
    }
}
