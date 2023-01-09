package io.prestosql.sql.planner.optimizations;

import io.prestosql.spi.plan.PlanNode;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import org.testng.annotations.Test;

import java.util.stream.IntStream;

public class TestCommonSubplanMerge extends BasePlanTest
{
    private static class CTEVisitor extends InternalPlanVisitor<Void, Integer>
    {

        @Override
        public Void visitPlan(PlanNode node, Integer context) {
            IntStream.range(0, context).boxed().forEach(i -> System.out.print("\t"));
            System.out.println(node.getClass() + " " + node.getHash());
            for (PlanNode source : node.getSources()) {
                source.accept(this, context+1);
            }
            return null;
        }
    }
    @Test
    public void testScore_1()
    {
        String query = "select o.orderkey from orders o, lineitem l1, lineitem l2 where l1.orderkey = o.orderkey and l2.orderkey = o.orderkey";
        Plan queryPlan = plan(query);
        System.out.println(queryPlan.getRoot());
        queryPlan.getRoot().accept(new CTEVisitor(), 0);
    }
}
