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

package io.prestosql.sql.planner.datapath.joingraph;

import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.spi.plan.*;
import java.util.HashSet;
import java.util.Set;

public class JoinMatrixHeaderNode
{
    public Set<String> tablesSet;
    public Set<String> viewsSet;
    public Set<String> partitionedBy;
    public PlanNode planNodeReference;
    public PlanNodeStatsEstimate nodeStatsEstimate;
    public double diff;
    public boolean replaced;

    //deep copy constructor
    public JoinMatrixHeaderNode(JoinMatrixHeaderNode joinMatrixHeaderNode)
    {
        this.tablesSet = new HashSet<String>();
        for (String tableName : joinMatrixHeaderNode.tablesSet) {
            this.tablesSet.add(new String(tableName));
        }

        this.viewsSet = new HashSet<String>();
        for (String viewName : joinMatrixHeaderNode.viewsSet) {
            this.viewsSet.add(new String(viewName));
        }

        this.partitionedBy = new HashSet<String>();
        for (String partitionKey : joinMatrixHeaderNode.partitionedBy) {
            this.partitionedBy.add(new String(partitionKey));
        }

        this.planNodeReference = joinMatrixHeaderNode.planNodeReference;
        this.nodeStatsEstimate = joinMatrixHeaderNode.nodeStatsEstimate;

        this.diff = joinMatrixHeaderNode.diff;
        this.replaced = joinMatrixHeaderNode.replaced;
    }

    public JoinMatrixHeaderNode(Set<String> tablesSet, Set<String> viewsSet, Set<String> partitionedBy, PlanNode planNodeReference, PlanNodeStatsEstimate nodeStatsEstimate)
    {
        this.tablesSet = tablesSet;
        this.viewsSet = viewsSet;
        this.partitionedBy = partitionedBy;
        this.planNodeReference = planNodeReference;
        this.nodeStatsEstimate = nodeStatsEstimate;
        this.diff = 0.0;
        this.replaced = false;
    }
}
