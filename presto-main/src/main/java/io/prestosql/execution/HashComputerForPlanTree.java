package io.prestosql.execution;


import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;

import java.util.List;

public class HashComputerForPlanTree
{
    private final PlanNode root;
    private static final Logger log = Logger.get(HashComputerForPlanTree.class);

    public HashComputerForPlanTree(PlanNode root)
    {
        this.root = root;
    }

    public Void computeHash()
    {
        return root.accept(new EquiJoinVisitor(), Long.parseLong("-1"));
    }

    // private static class EquiJoinVisitor extends InternalPlanVisitor<List<JoinNode>, Void>
    private static class EquiJoinVisitor extends InternalPlanVisitor<Void, Long>
    {
        /**


        @Override
        public List<JoinNode> visitJoin(JoinNode join, Void context)
        {
            if (!join.getCriteria().isEmpty()) {
                log.debug("equi join criteria: " + join.getCriteria());
                log.debug("left: " + join.getLeft() + ", right: " + join.getRight());
                List<JoinNode> children = visitPlan(join, context);
                ImmutableList.Builder build = ImmutableList.builder();
                build.add(join);
                build.addAll(children);
                return build.build();
            }
            return visitPlan(join, context);
        }

        @Override
        public List<JoinNode> visitPlan(PlanNode node, Void context)
        {
            ImmutableList.Builder build = ImmutableList.builder();
            for (PlanNode source : node.getSources()) {
                build.addAll(source.accept(this, context));
            }
            return build.build();
        }
         */

        @Override
        public Void visitPlan(PlanNode node, Long context)
        {
            //node.computeHash(context);
            for (PlanNode s : node.getSources()) {
                s.accept(this, node.computeHash(context));
            }
            log.debug("node: " + node + ", hash: " + node.computeHash(context));
            return null;
        }
    }
}
