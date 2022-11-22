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
//
package io.prestosql.sql.planner.datapath.globalplanner;

import io.airlift.log.Logger;
import io.prestosql.cost.PlanCostEstimate;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.planner.plan.CaptureLineage;
import io.prestosql.sql.planner.plan.IndexSourceNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class JoinInformationExtractor
{

    private static final Logger log = Logger.get(JoinInformationExtractor.class);

    public static JoinInformation extractJoinInformation(PlanNode root, StatsAndCosts planStatsAndCosts, JoinInformationPlanVisitor joinInformationPlanVisitor)
    {
        if (root == null) {
            return null;
        }

        for (PlanNode child : root.getSources()) {
            extractJoinInformation(child, planStatsAndCosts, joinInformationPlanVisitor);
        }

        root.accept(joinInformationPlanVisitor, planStatsAndCosts);

        return joinInformationPlanVisitor.joinInformation;
    }

    public static JoinInformation restrictJoinInformation (JoinInformation joinInformation, CaptureLineage.Lineage constraint)
    {
        Map<String, ScanNodeInfo> tableNameToScanNodesInfo = new HashMap<>();
        Map<String, FilterNodeInfo> tableNameToFilterNodesInfo = new HashMap<>();

        for (String tableName : joinInformation.tableNameToScanNodesInfo.keySet()) {
            if (constraint.getTableNames().contains(tableName)) {
                tableNameToScanNodesInfo.put(tableName, joinInformation.tableNameToScanNodesInfo.get(tableName));

                if (joinInformation.tableNameToFilterNodesInfo.containsKey(tableName)) {
                    tableNameToFilterNodesInfo.put(tableName, joinInformation.tableNameToFilterNodesInfo.get(tableName));
                }
            }
        }

        List<JoinNodeInfo> joinNodesInfo = new ArrayList<>();

        for (JoinNodeInfo joinNodeInfo : joinInformation.joinNodesInfo) {
            for (JoinNode.EquiJoinClause clause : constraint.getTableJoins()) {
                if (joinNodeInfo.equiJoinClause.equals(clause) || joinNodeInfo.equiJoinClause.flip().equals(clause)) {
                    joinNodesInfo.add(joinNodeInfo);
                }
            }
        }

        JoinInformation joinInformationNew = new JoinInformation();
        joinInformationNew.joinNodesInfo = joinNodesInfo;
        joinInformationNew.tableNameToScanNodesInfo = tableNameToScanNodesInfo;
        joinInformationNew.tableNameToFilterNodesInfo = tableNameToFilterNodesInfo;

        return joinInformationNew;
    }

    public static class JoinInformation
    {
        public List<JoinNodeInfo> joinNodesInfo; //to get the join conditions and build the join query graph
        public Map<String, ScanNodeInfo> tableNameToScanNodesInfo; //table name -> TableScanNode or IndexSourceNode
        public Map<String, FilterNodeInfo> tableNameToFilterNodesInfo;

        public JoinInformation()
        {
            this.joinNodesInfo = new ArrayList<>();
            this.tableNameToScanNodesInfo = new HashMap<>();
            this.tableNameToFilterNodesInfo = new HashMap<>();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            JoinInformation jinfo = (JoinInformation) o;
            if (joinNodesInfo.size() != jinfo.joinNodesInfo.size()
                    || tableNameToScanNodesInfo.size() != jinfo.tableNameToScanNodesInfo.size()
                    || tableNameToFilterNodesInfo.size() != jinfo.tableNameToFilterNodesInfo.size()) {
                return false;
            }

            int count = 0;
            for (JoinNodeInfo i : joinNodesInfo) {
                for (JoinNodeInfo j : jinfo.joinNodesInfo) {
                    if (i.equals2(j)) {
                        count++;
                    }
                }
            }
            if (count != joinNodesInfo.size())  {
                return false;
            }

            for (Map.Entry<String, ScanNodeInfo> e : this.tableNameToScanNodesInfo.entrySet()) {
                String key = e.getKey();
                ScanNodeInfo jinfoValue = jinfo.tableNameToScanNodesInfo.get(key);
                if (jinfoValue == null || !jinfoValue.equals(e.getValue())) {
                    return false;
                }
            }

            for (Map.Entry<String, FilterNodeInfo> e : this.tableNameToFilterNodesInfo.entrySet()) {
                String key = e.getKey();
                FilterNodeInfo jinfoValue = jinfo.tableNameToFilterNodesInfo.get(key);
                if (jinfoValue == null || !jinfoValue.equals(e.getValue())) {
                    return false;
                }
            }

            return true;
        }
    }

    public static class JoinNodeInfo
    {
        public JoinNode.EquiJoinClause equiJoinClause;
        public PlanNodeStatsEstimate nodeStatsEstimate;
        public PlanCostEstimate nodeCostsEstmate;

        public JoinNodeInfo(JoinNode.EquiJoinClause equiJoinClause, PlanNodeStatsEstimate nodeStatsEstimate, PlanCostEstimate nodeCostsEstmate)
        {
            this.equiJoinClause = equiJoinClause;
            this.nodeStatsEstimate = nodeStatsEstimate;
            this.nodeCostsEstmate = nodeCostsEstmate;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            JoinNodeInfo other = (JoinNodeInfo) o;
            return this.equiJoinClause.equals2(other.equiJoinClause)
                            && this.nodeCostsEstmate.equals(other.nodeCostsEstmate)
                            && this.nodeStatsEstimate.equals(other.nodeStatsEstimate);
        }

        public boolean equals2(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            JoinNodeInfo other = (JoinNodeInfo) o;
            return this.equiJoinClause.equals2(other.equiJoinClause);
        }
    }

    public static class ScanNodeInfo
    {
        public PlanNode scanNodeRef;
        public PlanNodeStatsEstimate nodeStatsEstimate;

        public ScanNodeInfo(PlanNode scanNodeRef, PlanNodeStatsEstimate nodeStatsEstimate)
        {
            this.scanNodeRef = scanNodeRef;
            this.nodeStatsEstimate = nodeStatsEstimate;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ScanNodeInfo other = (ScanNodeInfo) o;

            boolean b = false;
            if (scanNodeRef instanceof TableScanNode) {
                TableScanNode thisScan = (TableScanNode) scanNodeRef;
                TableScanNode otherScan = (TableScanNode) other.scanNodeRef;
                b = thisScan.isSymbolsEqual(otherScan.getOutputSymbols(), thisScan.getOutputSymbols())
                        && thisScan.isSourcesEqual(otherScan.getSources(), thisScan.getSources())
                        && thisScan.isPredicateSame(otherScan);
            }
            return b;
        }
    }

    public static class FilterNodeInfo
    {
        public PlanNode filterNodeRef;
        public PlanNodeStatsEstimate nodeStatsEstimate;

        public FilterNodeInfo(PlanNode filterNodeRef, PlanNodeStatsEstimate nodeStatsEstimate)
        {
            this.filterNodeRef = filterNodeRef;
            this.nodeStatsEstimate = nodeStatsEstimate;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FilterNode thisFilter = (FilterNode) filterNodeRef;
            FilterNode otherFilter = (FilterNode) ((FilterNodeInfo) o).filterNodeRef;

            boolean b = thisFilter.isPredicateSame(otherFilter);

            /**
             * is it required to compare the sources also?
             * Debug and check
             */
            return b;
        }
    }

    public static class JoinInformationPlanVisitor
            extends InternalPlanVisitor<Void, StatsAndCosts> //<return type, second parameter type)
    {
        private final JoinInformation joinInformation = new JoinInformation();

        @Override
        public Void visitPlan(PlanNode node, StatsAndCosts planStatsAndCosts)
        {
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, StatsAndCosts planStatsAndCosts)
        {
            if (node.getCriteria().isEmpty()) {
                log.debug("cannot handle this join node. Leaving it out from comparison...");
                return null;
            }
            checkArgument(node.getCriteria().size() > 0);

            node.getCriteria().forEach(c -> log.debug(String.valueOf(c)));

            String conditionLeftSideSymbolRelativeName = node.getCriteria().get(0).getLeft().getName();
            Symbol conditionLeftSide = node.symbolNamesToAbsoluteNames.get(conditionLeftSideSymbolRelativeName);

            String conditionRightSideSymbolRelativeName = node.getCriteria().get(0).getRight().getName();
            Symbol conditionRightSide = node.symbolNamesToAbsoluteNames.get(conditionRightSideSymbolRelativeName);

            log.debug("left symbol: " + conditionLeftSide);
            log.debug("right symbol: " + conditionRightSide);

            if (conditionLeftSide != null && conditionRightSide != null) {
                joinInformation.joinNodesInfo.add(new JoinNodeInfo(new JoinNode.EquiJoinClause(conditionLeftSide, conditionRightSide),
                        planStatsAndCosts.getStats().get(node.getId()), planStatsAndCosts.getCosts().get(node.getId())));
            }
            else {
                log.debug("cannot handle this join node. Leaving it out from comparison...");
            }

            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, StatsAndCosts planStatsAndCosts)
        {
            String tableNameKey = node.getTable().getFullyQualifiedName();
            joinInformation.tableNameToScanNodesInfo.put(tableNameKey, new JoinInformationExtractor.ScanNodeInfo(node, planStatsAndCosts.getStats().get(node.getId())));
            return null;
        }

        //@Override
        public Void visitIndexSource(IndexSourceNode node, StatsAndCosts planStatsAndCosts)
        {
            String[] parts = node.getTableHandle().getConnectorHandle().toString().split(" ");
            String tableName = node.getTableHandle().getCatalogName().toString() + "." + parts[0];
            joinInformation.tableNameToScanNodesInfo.put(tableName, new JoinInformationExtractor.ScanNodeInfo(node, planStatsAndCosts.getStats().get(node.getId())));
            return null;
        }

        @Override
        public Void visitFilter(FilterNode filterNode, StatsAndCosts planStatsAndCosts)
        {
            PlanNode sourceNode = filterNode.getSource();

            if (sourceNode instanceof TableScanNode) {
                String tableNameKey = ((TableScanNode) sourceNode).getTable().getFullyQualifiedName();
                joinInformation.tableNameToFilterNodesInfo.put(tableNameKey, new JoinInformationExtractor.FilterNodeInfo(filterNode, planStatsAndCosts.getStats().get(filterNode.getId())));

                if (planStatsAndCosts.getStats().get(filterNode.getId()) == null) {
                    log.debug("stats is null");
                }

            }
            else if (sourceNode instanceof  IndexSourceNode) {
                String[] parts = ((IndexSourceNode) sourceNode).getTableHandle().getConnectorHandle().toString().split(" ");
                String tableName = ((IndexSourceNode) sourceNode).getTableHandle().getCatalogName().toString() + "." + parts[0];
                joinInformation.tableNameToFilterNodesInfo.put(tableName, new JoinInformationExtractor.FilterNodeInfo(filterNode, planStatsAndCosts.getStats().get(filterNode.getId())));

                if (planStatsAndCosts.getStats().get(filterNode.getId()) == null) {
                    log.debug("stats is null");
                }
            }
            else {
                //do nothing
            }

            return null;
        }

    }
}
