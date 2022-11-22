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

import io.airlift.log.Logger;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.sql.planner.datapath.globalplanner.JoinInformationExtractor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class JoinMatrix
{
    private static final Logger log = Logger.get(JoinMatrix.class);

    public List<JoinMatrixHeaderNode> headerNodes;

    public JoinMatrixEntryNode[][] adjacencyMatrix;

    public JoinMatrix(JoinMatrix joinMatrix) {
        this.headerNodes = new ArrayList<>();
        this.adjacencyMatrix = new JoinMatrixEntryNode[joinMatrix.headerNodes.size()][joinMatrix.headerNodes.size()];

        for (JoinMatrixHeaderNode headerNode : joinMatrix.headerNodes) {
            this.headerNodes.add(new JoinMatrixHeaderNode(headerNode));
        }

        for (int i = 0; i < joinMatrix.headerNodes.size(); i++) {
            for (int j = 0; j < joinMatrix.headerNodes.size(); j++) {
                this.adjacencyMatrix[i][j] = new JoinMatrixEntryNode(joinMatrix.adjacencyMatrix[i][j]); //call the deep copy constructor
            }
        }
    }

    public JoinMatrix(JoinMatrix joinMatrix, int indexToBeRemoved)
    {
        int newAdjacencyMatrixSize = joinMatrix.headerNodes.size() - 1;
        this.headerNodes = new ArrayList<>();
        this.adjacencyMatrix = new JoinMatrixEntryNode[newAdjacencyMatrixSize][newAdjacencyMatrixSize];

        for (int i = 0; i < joinMatrix.headerNodes.size(); i++) {
            if (i == indexToBeRemoved) {
                continue;
            }
            this.headerNodes.add(new JoinMatrixHeaderNode(joinMatrix.headerNodes.get(i)));
        }

        int currentRowIndex = 0;
        int currentColumnIndex = 0;
        for (int i = 0; i < joinMatrix.headerNodes.size(); i++) {
            currentColumnIndex = 0;
            if (i == indexToBeRemoved) {
                continue;
            }
            for (int j = 0; j < joinMatrix.headerNodes.size(); j++) {
                if (j == indexToBeRemoved) {
                    continue;
                }
                this.adjacencyMatrix[currentRowIndex][currentColumnIndex] = new JoinMatrixEntryNode(joinMatrix.adjacencyMatrix[i][j]); //call the deep copy constructor
                currentColumnIndex++;
            }
            currentRowIndex++;
        }
    }

    public JoinMatrix(JoinInformationExtractor.JoinInformation joinInformation, Map<String, PlanNode> globalPlanTableNameToScanNodeRef)
    {
        List<JoinInformationExtractor.JoinNodeInfo> joinNodesInfo = joinInformation.joinNodesInfo;
        Map<String, JoinInformationExtractor.ScanNodeInfo> tableNameToScanNodeInfo = joinInformation.tableNameToScanNodesInfo;
        int numOfTablesIncludedInQuery = tableNameToScanNodeInfo.size();

        headerNodes = new ArrayList<>();
        adjacencyMatrix = new JoinMatrixEntryNode[numOfTablesIncludedInQuery][numOfTablesIncludedInQuery];
        //initialize adjacency matrix with false connections
        for (int i = 0; i < numOfTablesIncludedInQuery; i++) {
            for (int j = 0; j < numOfTablesIncludedInQuery; j++) {
                adjacencyMatrix[i][j] = new JoinMatrixEntryNode(false, null, null);
            }
        }

        //build header with the joinRelations and scanNodeReferences
        for (Map.Entry<String, JoinInformationExtractor.ScanNodeInfo> tableNameToScanNodeInfoEntry : tableNameToScanNodeInfo.entrySet()) {
            String tableName = tableNameToScanNodeInfoEntry.getKey();
            //TODO update the cost here to
            headerNodes.add(new JoinMatrixHeaderNode(new HashSet<String>(Arrays.asList(tableName)), new HashSet<>(), new HashSet<>(), globalPlanTableNameToScanNodeRef.get(tableName), tableNameToScanNodeInfoEntry.getValue().nodeStatsEstimate));//REFERENCE IS HERE use the reference from the global plan map "globalPlanTableNameToScanNodeRef" not the local plan map "tableNameToScanNode"
        }

        //build matrix with the joinConditions (directed graph)
        for (JoinInformationExtractor.JoinNodeInfo joinNodeInfo : joinNodesInfo) {
            int leftRelationIndex = findTableIndexInHeader(joinNodeInfo.equiJoinClause.getLeft().tableName);
            int rightRelationIndex = findTableIndexInHeader(joinNodeInfo.equiJoinClause.getRight().tableName);
            adjacencyMatrix[leftRelationIndex][rightRelationIndex] = new JoinMatrixEntryNode(true, joinNodeInfo.equiJoinClause, joinInformation.tableNameToScanNodesInfo.get(joinNodeInfo.equiJoinClause.getRight().tableName).nodeStatsEstimate);
            if (joinNodeInfo.equiJoinClause == null) {
//                System.out.println("This shouldn't be null");
            }
            if (joinNodeInfo.nodeStatsEstimate == null) {
//                System.out.println("HEREEE??");
            }
//            System.out.println("JOIN COSTT: " + joinNodeInfo.nodeStatsEstimate.getOutputRowCount());
        }
    }

    public int findTableIndexInHeader(String tableName)
    {
        for (int i = 0; i < headerNodes.size(); i++) {
            JoinMatrixHeaderNode headerNode = headerNodes.get(i);

            if (headerNode.tablesSet.size() == 1 && headerNode.tablesSet.contains(tableName)) {
                return i;
            }
        }
        log.debug("ERRORR " + tableName);
        return -1;
    }

    public int findTableSetIndexInHeader(String tableName)
    {
        for (int i = 0; i < headerNodes.size(); i++) {
            JoinMatrixHeaderNode headerNode = headerNodes.get(i);

            if (headerNode.tablesSet.contains(tableName)) {
                return i;
            }
        }
        log.debug("ERRORR " + tableName);
        return -1;
    }

    public static class Pair
    {
        int i;
        int j;
        Optional<String> partOfView;

        public Pair(int i, int j) {
            this.i = i;
            this.j = j;
            this.partOfView = Optional.empty();
        }

        public Pair(int i, int j, String view) {
            this.i = i;
            this.j = j;
            this.partOfView = Optional.of(view);
        }

        public int getI()
        {
            return i;
        }

        public int getJ()
        {
            return j;
        }

        public Optional<String> getPartOfView() {
            return partOfView;
        }
    }

    //takes joinMatrix -> return cell index (i,j) that has one under the condition that i != J
    private static Pair selectRandomJoin(JoinMatrix joinMatrix)
    {
        List<Pair> connectedEntriesIndices = new ArrayList<>();

        for (int i = 0; i < joinMatrix.headerNodes.size(); i++) {
            for (int j = 0; j < joinMatrix.headerNodes.size(); j++) {
                if (i != j && joinMatrix.adjacencyMatrix[i][j].connected) {
                    connectedEntriesIndices.add(new Pair(i, j));
                }
            }
        }

        //select random entry from connectedEntries by generating a random index between 0 and connectedEntries.size() - 1
        int min = 0;
        int max = connectedEntriesIndices.size() - 1;
        int randomIndex = ThreadLocalRandom.current().nextInt(min, max + 1);

        return connectedEntriesIndices.get(randomIndex);
    }

    //twoRowsUnion -> takes join matrix, row1 = i, row2 = j -> find union of both rows and update header
    private static JoinMatrix twoRowsUnion(JoinMatrix joinMatrix, Pair twoDIndex, JoinNode joinNodeReference, Optional<String> partOfView, double diff)
    {
        int row1Index = twoDIndex.i;
        int row2Index = twoDIndex.j;

        JoinMatrix deepCopiedJoinMatrix = new JoinMatrix(joinMatrix);

        //copy row1 to row2
        for (int j = 0; j < deepCopiedJoinMatrix.headerNodes.size(); j++) {
            if (deepCopiedJoinMatrix.adjacencyMatrix[row1Index][j].connected && !deepCopiedJoinMatrix.adjacencyMatrix[row2Index][j].connected) {
                deepCopiedJoinMatrix.adjacencyMatrix[row2Index][j] = new JoinMatrixEntryNode(deepCopiedJoinMatrix.adjacencyMatrix[row1Index][j]);
            }
        }

        //copy row2 to row1
        for (int j = 0; j < deepCopiedJoinMatrix.headerNodes.size(); j++) {
            if (deepCopiedJoinMatrix.adjacencyMatrix[row2Index][j].connected && !deepCopiedJoinMatrix.adjacencyMatrix[row1Index][j].connected) {
                deepCopiedJoinMatrix.adjacencyMatrix[row1Index][j] = new JoinMatrixEntryNode(deepCopiedJoinMatrix.adjacencyMatrix[row2Index][j]);
            }
        }

        //update the header (union of both row1 and row2 headers)
        //note that here only the tablesSet got updated, the headerNodes.get(row1Index).planNodeReference will be updated in the search algorithm function (after searching the global plan for an existing join)
        Set<String> row1TablesSet = deepCopiedJoinMatrix.headerNodes.get(row1Index).tablesSet;
        Set<String> row2TablesSet = deepCopiedJoinMatrix.headerNodes.get(row2Index).tablesSet;
        deepCopiedJoinMatrix.headerNodes.get(row1Index).tablesSet.addAll(row2TablesSet);
        deepCopiedJoinMatrix.headerNodes.get(row2Index).tablesSet.addAll(row1TablesSet);

        Set<String> row1ViewsSet = deepCopiedJoinMatrix.headerNodes.get(row1Index).viewsSet;
        Set<String> row2ViewsSet = deepCopiedJoinMatrix.headerNodes.get(row2Index).viewsSet;
        deepCopiedJoinMatrix.headerNodes.get(row1Index).viewsSet.addAll(row2TablesSet);
        deepCopiedJoinMatrix.headerNodes.get(row2Index).viewsSet.addAll(row1TablesSet);

        if (partOfView.isPresent()) {
            deepCopiedJoinMatrix.headerNodes.get(row1Index).viewsSet.add(partOfView.get());
            deepCopiedJoinMatrix.headerNodes.get(row2Index).viewsSet.add(partOfView.get());
        }

        deepCopiedJoinMatrix.headerNodes.get(row1Index).planNodeReference = joinNodeReference;
        deepCopiedJoinMatrix.headerNodes.get(row2Index).planNodeReference = joinNodeReference; //NULL REFERENCE IS HERE

        deepCopiedJoinMatrix.headerNodes.get(row1Index).partitionedBy = new HashSet<>();
        deepCopiedJoinMatrix.headerNodes.get(row1Index).partitionedBy.add(deepCopiedJoinMatrix.adjacencyMatrix[row1Index][row2Index].equiJoinClause.getLeft().getName());

        deepCopiedJoinMatrix.headerNodes.get(row2Index).partitionedBy = new HashSet<>();
        deepCopiedJoinMatrix.headerNodes.get(row2Index).partitionedBy.add(deepCopiedJoinMatrix.adjacencyMatrix[row1Index][row2Index].equiJoinClause.getLeft().getName());

        deepCopiedJoinMatrix.headerNodes.get(row1Index).diff = diff;
        deepCopiedJoinMatrix.headerNodes.get(row2Index).diff = diff;

        return deepCopiedJoinMatrix;
    }


    //twoColumnsUnion -> takes join matrix, column1 = i, column2 = j -> find union of both columns (DON't update header as it the same header for vertical and horizontal)
    private static JoinMatrix twoColumnsUnion(JoinMatrix joinMatrix, Pair twoDIndex)
    {
        int column1Index = twoDIndex.i;
        int column2Index = twoDIndex.j;

        JoinMatrix deepCopiedJoinMatrix = new JoinMatrix(joinMatrix);

        //copy column1 to column2
        for (int i = 0; i < deepCopiedJoinMatrix.headerNodes.size(); i++) {
            if (deepCopiedJoinMatrix.adjacencyMatrix[i][column1Index].connected && !deepCopiedJoinMatrix.adjacencyMatrix[i][column2Index].connected) {
                deepCopiedJoinMatrix.adjacencyMatrix[i][column2Index] = new JoinMatrixEntryNode(deepCopiedJoinMatrix.adjacencyMatrix[i][column1Index]);
            }
        }

        //copy column2 to column1
        for (int i = 0; i < deepCopiedJoinMatrix.headerNodes.size(); i++) {
            if (deepCopiedJoinMatrix.adjacencyMatrix[i][column2Index].connected && !deepCopiedJoinMatrix.adjacencyMatrix[i][column1Index].connected) {
                deepCopiedJoinMatrix.adjacencyMatrix[i][column1Index] = new JoinMatrixEntryNode(deepCopiedJoinMatrix.adjacencyMatrix[i][column2Index]);
            }
        }

        return deepCopiedJoinMatrix;
    }

    //rowAndColumnCollapse -> takes a join matrix and index i -> return NEW join matrix where this column = i and row = i is removed from both the matrix and the header
    private static JoinMatrix rowAndColumnCollapse(JoinMatrix joinMatrix, int indexToBeRemoved)
    {
        return new JoinMatrix(joinMatrix, indexToBeRemoved);
    }

    //mergeJoin -> takes a join matrix and [i,j] -> returns new join matrix where the given join is merged
    //call rowUnion(i, j) then columnUnion(i, j) then rowAndColumnCollapse(i)
    public static JoinMatrix mergeJoin(JoinMatrix joinMatrix, Pair twoDIndex, JoinNode joinNodeReference, Optional<String> partOfView, double diff)
    {
        return rowAndColumnCollapse(twoColumnsUnion(twoRowsUnion(joinMatrix, twoDIndex, joinNodeReference, partOfView, diff), twoDIndex), twoDIndex.i);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < headerNodes.size(); i++) {
            //sb.append(headerNodes.get(i).tablesSet + " " + headerNodes.get(i).planNodeReference.toString() + "||");
            sb.append(i+" = "+headerNodes.get(i).tablesSet+"\n");
        }
        sb.append("\n");
        for (int i = 0; i < headerNodes.size(); i++) {
            for (int j = 0; j < headerNodes.size(); j++) {
                //sb.append(adjacencyMatrix[i][j].connected);
                if (adjacencyMatrix[i][j].connected) {
                    sb.append("i= "+i+" j = "+j+"= clause: "+adjacencyMatrix[i][j].equiJoinClause.toString() + "\n");
                }
             }
        }
        return sb.toString();
    }

    public String getHashString()
    {
        List<String> joinsAsStrings = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < headerNodes.size(); i++) {
            for (int j = 0; j < headerNodes.size(); j++) {
                if (i != j && adjacencyMatrix[i][j].connected) {
                    joinsAsStrings.add(setAsString(headerNodes.get(i).tablesSet) + "_" + setAsString(headerNodes.get(j).tablesSet) + "__");
                }
            }
        }

        Collections.sort(joinsAsStrings);

        for (String member : joinsAsStrings) {
            sb.append(member);
        }
        sb.append(headerNodes.size());
        //the join matrix is defined by the set of join operators it contains, and the size of the join matrix
        return sb.toString();
    }

    String setAsString(Set<String> set)
    {
        StringBuilder sb = new StringBuilder();
        List<String> list = new ArrayList<String>();
        list.addAll(set);
        Collections.sort(list);
        for (String member : list) {
            sb.append(member);
        }
        return sb.toString();
    }


//    public JoinMatrix(Set<String> queryRelationsSet, GlobalPlaner globalPlaner) {
//        //initialize the header, one header node for each relation included in the new query
//        for (String relationName: queryRelationsSet) {
//            Set<String> nodeRelationsSet = new HashSet<String>(Arrays.asList(relationName));
//            PlanNode planNodeReference = globalPlaner.getTableScanNode(relationName);
//            JoinMatrixHeaderNode headerNode = new JoinMatrixHeaderNode(nodeRelationsSet, planNodeReference);
//            headerNodes.add(new JoinMatrixHeaderNode());
//        }
//    }
}
