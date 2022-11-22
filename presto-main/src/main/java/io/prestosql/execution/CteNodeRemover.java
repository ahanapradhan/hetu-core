package io.prestosql.execution;

import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;

import java.util.ArrayList;
import java.util.List;

public class CteNodeRemover
{
    private final PlanNode root;

    public CteNodeRemover(PlanNode root)
    {
        this.root = root;
    }

    public PlanNode removeCTE()
    {
        return root.accept(new CteVisitor(), null);
    }

    private static class CteVisitor extends InternalPlanVisitor<PlanNode, Void>
    {
        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            List<PlanNode> children = new ArrayList<>();
            for (PlanNode source : node.getSources()) {
                source.accept(this, context);
                if (source instanceof CTEScanNode) {
                    PlanNode child = ((CTEScanNode) source).getSource();
                    children.add(child);
                }
                else {
                   children.add(source);
                }
            }
            return node.replaceChildren(children);
        }
    }
}
