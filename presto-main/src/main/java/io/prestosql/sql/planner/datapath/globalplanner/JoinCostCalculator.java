/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.sql.planner.datapath.globalplanner;

import com.google.common.graph.Traverser;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.spi.plan.*;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;

public class JoinCostCalculator
{
    public static Double calculateJoinCost(PlanNode root, StatsAndCosts planStatsAndCosts)
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);

        double planJoinCost = 0;
        for (PlanNode node : planIterator) {
            planJoinCost += node.accept(new JoinCostVisitor(), planStatsAndCosts);
        }

        return planJoinCost;
    }

    private static class JoinCostVisitor
            extends InternalPlanVisitor<Double, StatsAndCosts> //<return type, second parameter type)
    {
        @Override
        public Double visitPlan(PlanNode node, StatsAndCosts planStat)
        {
            return 0.0;
        }

        @Override
        public Double visitJoin(JoinNode node, StatsAndCosts planStats)
        {
            if (!Double.isNaN(planStats.getStats().get(node.getId()).getOutputRowCount())) {
                return planStats.getStats().get(node.getId()).getOutputRowCount();
            }
            return 0.0;
        }
    }

    public static Double calculatePlanCost(PlanNode root, StatsAndCosts planStatsAndCosts)
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);

        double planJoinCost = 0;
        for (PlanNode node : planIterator) {
            planJoinCost += node.accept(new PlanCostVisitor(), planStatsAndCosts);
        }

        return planJoinCost;
    }

    private static class PlanCostVisitor
            extends InternalPlanVisitor<Double, StatsAndCosts> //<return type, second parameter type)
    {

        @Override
        public Double visitPlan(PlanNode node, StatsAndCosts planStat)
        {
            return 0.0;
        }

        @Override
        public Double visitJoin(JoinNode node, StatsAndCosts planStats)
        {
            if (!Double.isNaN(planStats.getStats().get(node.getId()).getOutputRowCount())) {
                return planStats.getStats().get(node.getId()).getOutputRowCount();
            }
            return 0.0;
        }
    }
}
