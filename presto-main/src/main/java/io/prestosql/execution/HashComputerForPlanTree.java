package io.prestosql.execution;


import io.airlift.log.Logger;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HashComputerForPlanTree
{
    private final PlanNode root;
    private static final Logger log = Logger.get(HashComputerForPlanTree.class);

    public HashComputerForPlanTree(PlanNode root)
    {
        this.root = root;
    }

    public Map<Integer, Set<PlanNode>> computeHash()
    {
        EquiJoinVisitor visitor = new EquiJoinVisitor();
        root.accept(visitor, null);
        return visitor.hashCounter;
    }

    // private static class EquiJoinVisitor extends InternalPlanVisitor<List<JoinNode>, Void>
    private static class EquiJoinVisitor extends InternalPlanVisitor<Void, Long>
    {
        Map<Integer, Set<PlanNode>> hashCounter = new HashMap<>();
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
                s.accept(this, context);
            }
            int hash = node.getHash();
            log.debug("node: " + node + ", hash: " + hash);
            Set<PlanNode> nodes = hashCounter.containsKey(hash) ? hashCounter.get(hash) : new HashSet<>();
            nodes.add(node);
            hashCounter.put(hash, nodes);
            return null;
        }
    }
}
