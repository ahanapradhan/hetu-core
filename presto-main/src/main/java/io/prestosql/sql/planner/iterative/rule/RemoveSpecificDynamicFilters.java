package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.sql.DynamicFilters.Function.NAME;
import static java.util.Objects.requireNonNull;

/**
 * Dynamic Filters generated as well as consumed within a dynamically identified CTE node causes execution error in case of multinode
 * cluster execution scenario when MergeCommonSubplans optimization is enabled.
 * This optimizer is added to remove those specific dynamic filters.
 * Therefore, it is for a hackish bugfix.
 * Remove it if Dynamic filters inside CTE nodes causes no execution conflict.
 */
public class RemoveSpecificDynamicFilters
    implements PlanOptimizer
{
    private static final Logger log = Logger.get(RemoveSpecificDynamicFilters.class);
    private Set<String> removedDynamicFilterIds;
    private final Metadata metadata;

    public RemoveSpecificDynamicFilters(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (SystemSessionProperties.isSubplanMergeEnabled(session)) {
            this.removedDynamicFilterIds = plan.accept(new DFRemoverForCTEs(), null);
            return plan.accept(new PlanReWriter(removedDynamicFilterIds, new LogicalRowExpressions(
                    new RowExpressionDeterminismEvaluator(metadata),
                    new FunctionResolution(metadata.getFunctionAndTypeManager()),
                    metadata.getFunctionAndTypeManager())), Boolean.FALSE);
        }
        return plan;
    }

    private static class PlanReWriter  extends InternalPlanVisitor<PlanNode, Boolean>
    {
        private final LogicalRowExpressions logicalRowExpressions;
        private final Set<String> removedDynamicFilterIds;

        public PlanReWriter(Set<String> removedDynamicFilterIds, LogicalRowExpressions logicalRowExpressions)
        {
            this.logicalRowExpressions = logicalRowExpressions;
            this.removedDynamicFilterIds = removedDynamicFilterIds;
        }

        @Override
        public PlanNode visitCTEScan(CTEScanNode node, Boolean context)
        {
            PlanNode source = node.getSource().accept(this, Boolean.TRUE);
            return node.replaceChildren(ImmutableList.of(source));
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Boolean context)
        {
            List<PlanNode> newSources = new ArrayList<>();
            for (PlanNode source : node.getSources()) {
                newSources.add(source.accept(this, context));
            }
            return node.replaceChildren(newSources);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Boolean context)
        {
            PlanNode source = node.getSource().accept(this, context);
            RowExpression original = node.getPredicate();
            if (original instanceof CallExpression) {
                if(context) {
                    CallExpression callExpression = (CallExpression) original;
                    if (callExpression.getDisplayName().equals(NAME)) {
                        return source;
                    }
                }
             }
            return node.replaceChildren(ImmutableList.of(source));
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Boolean context)
        {
            PlanNode left = node.getLeft().accept(this, context);
            PlanNode right = node.getRight().accept(this, context);
            Map<String, Symbol> dfs = new HashMap<>(node.getDynamicFilters());

            for (String df : removedDynamicFilterIds) {
                dfs.remove(df);
            }
            return node.modifyDynamicFilter(ImmutableList.of(left, right), dfs);
        }


        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, Boolean context)
        {
            PlanNode source = node.getSource().accept(this, context);
            PlanNode filterSource = node.getFilteringSource().accept(this, context);
            String filter = node.getDynamicFilterId().isPresent() ? node.getDynamicFilterId().get() : "-";
            if (removedDynamicFilterIds.contains(filter)) {
                Optional<String> dynamicFilters = node.getDynamicFilterId()
                        .filter(entry -> !removedDynamicFilterIds.contains(entry));
                return new SemiJoinNode(
                        node.getId(),
                        source,
                        filterSource,
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashSymbol(),
                        node.getFilteringSourceHashSymbol(),
                        node.getDistributionType(),
                        dynamicFilters);
            }
            return node.replaceChildren(ImmutableList.of(source, filterSource));
        }
    }

    private static class DFRemoverForCTEs extends InternalPlanVisitor<Set<String>, Void>
    {
        private boolean remove = false;

        @Override
        public Set<String> visitPlan(PlanNode node, Void context) {
            Set<String> builder = new HashSet<>();
            for (PlanNode source : node.getSources()) {
                builder.addAll(source.accept(this, context));
            }
            return builder;
        }

        @Override
        public Set<String> visitJoin(JoinNode node, Void context) {
            Set<String> builder = new HashSet<>();
            if (remove) {
                builder.addAll(node.getDynamicFilters().keySet());
            }
            for (PlanNode source : node.getSources()) {
                builder.addAll(source.accept(this, context));
            }
            return builder;
        }

        @Override
        public Set<String> visitSemiJoin(SemiJoinNode node, Void context) {
            Set<String> builder = new HashSet<>();
            if (remove && node.getDynamicFilterId().isPresent()) {
                builder.add(node.getDynamicFilterId().get());
            }
            for (PlanNode source : node.getSources()) {
                builder.addAll(source.accept(this, context));
            }
            return builder;
        }

        @Override
        public Set<String> visitCTEScan(CTEScanNode node, Void context) {
            this.remove = true;
            Set<String> builder = new HashSet<>();
            builder.addAll(node.getSource().accept(this, context));
            this.remove = false;
            return builder;
        }

    }


}
