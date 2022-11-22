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
import java.util.List;

public class JoinMatrixEntryNode
{
    public boolean connected;
    public JoinNode.EquiJoinClause equiJoinClause;
    public PlanNodeStatsEstimate nodeStatsEstimate;


    //deep copy ocn
    public JoinMatrixEntryNode(JoinMatrixEntryNode joinMatrixEntryNode)
    {
        this.connected = joinMatrixEntryNode.connected;

        //no need to deep copy these attributes as they will not be modified when join matrix is merged
        this.equiJoinClause = joinMatrixEntryNode.equiJoinClause;
        this.nodeStatsEstimate = joinMatrixEntryNode.nodeStatsEstimate;
    }

    public JoinMatrixEntryNode(boolean connected, JoinNode.EquiJoinClause equiJoinClause, PlanNodeStatsEstimate nodeStatsEstimate)
    {
        this.connected = connected;
        this.equiJoinClause = equiJoinClause;
        this.nodeStatsEstimate = nodeStatsEstimate;
    }
}
