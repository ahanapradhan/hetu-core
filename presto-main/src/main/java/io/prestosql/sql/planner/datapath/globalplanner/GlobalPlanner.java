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

import io.airlift.log.Logger;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.execution.CTEcreatorForCommonJoins;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.TableScanNode;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GlobalPlanner {

    private static final Logger log = Logger.get(GlobalPlanner.class);

    public static final String CTE_NODE_PREFIX = "$cte_node";

    Map<String, PlanNode> tableNameToScanNodeRef = new HashMap<>();

    public void reset()
    {
        tableNameToScanNodeRef.clear();
    }

    public JoinInformationExtractor.JoinInformation getJoinInfoQueryOrder(PlanNode root, StatsAndCosts planStatsAndCosts)
    {
        SymbolsNameAdapter.adaptSymbolNamesToGlobalNaming(root, new SymbolsNameAdapter.SymbolsNameAdapterPlanVisitor());

        //extract included relations and join conditions from the new query plan
        //add the new relations (scan nodes) to the global plan scanNodes map "relationNameToScanNodeRef"
        JoinInformationExtractor.JoinInformation joinInformation = JoinInformationExtractor.extractJoinInformation(root,
                planStatsAndCosts, new JoinInformationExtractor.JoinInformationPlanVisitor());

        for (Map.Entry<String, JoinInformationExtractor.ScanNodeInfo> tableNameToScanNodeInfoEntry : joinInformation.tableNameToScanNodesInfo.entrySet()) {
            //scanNodeEntry.getKey() -> table name
            //scanNodeEntry.getValue() -> TableScanNode
            if (!tableNameToScanNodeRef.containsKey(tableNameToScanNodeInfoEntry.getKey())) {
                tableNameToScanNodeRef.put(tableNameToScanNodeInfoEntry.getKey(), tableNameToScanNodeInfoEntry.getValue().scanNodeRef);
            }
        }

        return joinInformation;
    }

    @NotNull
    private static Map<Integer, ArrayList<Set<String>>> processJoinGraphMap(Map<Integer, JoinInformationExtractor.JoinInformation> JoinGraphMap) {
        Map<Integer, ArrayList<Set<String>>> ProcessedJoinGraphMap = new HashMap<>();

        for (Integer K : JoinGraphMap.keySet()) {
            JoinInformationExtractor.JoinInformation joinInfo = JoinGraphMap.get(K);
            log.debug("The Query Id is " + K);
            ArrayList<Set<String>> joinColsList = new ArrayList<>();
            for (JoinInformationExtractor.JoinNodeInfo joinNodeInfo : joinInfo.joinNodesInfo) {
                String joinClause = joinNodeInfo.equiJoinClause.toString();

                Set<String> joinCols = new HashSet<>();
                if (joinClause.contains("=")) {
                    String columns[] = joinClause.split("=");
                    for (String col : columns) {
                        joinCols.add(TableScanNode.getActualColName(col.trim()));
                    }
                    joinColsList.add(joinCols);
                }
            }
            if (joinColsList.size() > 0)
                ProcessedJoinGraphMap.put(K, joinColsList);
        }
        return ProcessedJoinGraphMap;
    }

    public static List<CTEcreatorForCommonJoins.JoinCteNodeInfo> createOverlapMatches(Map<Integer, JoinInformationExtractor.JoinInformation> joinGraphMap)
    {
        ArrayList<Integer> vertices = new ArrayList<>();
        Map<Integer, ArrayList<Set<String>>> joinColsJoinGraphMap = processJoinGraphMap(joinGraphMap);

        HashMap<Integer, HashSet<Integer>> vertexCluster = makeVertexCluster(joinGraphMap, vertices);
        log.debug("Cluster Map is " + vertexCluster);

        ArrayList<Integer> isolatedVertices = getIsolatedVertices(vertices, vertexCluster);
        log.debug("Isolated vertices " + isolatedVertices);

        Map<Integer, Integer> clusterVertexMapping = createClusterVertexMapping(vertexCluster, isolatedVertices);
        log.debug("cluster Vertex Mapping" + clusterVertexMapping);

        vertices.clear();
        vertices.addAll(clusterVertexMapping.keySet());

        Set<Integer> distinctCtes = getDistinctJoinNodesForCte(vertices, joinColsJoinGraphMap, clusterVertexMapping);//vertexCluster.keySet(); //

        Set<Integer> copy = new HashSet<>(distinctCtes);
        copy.removeAll(vertexCluster.keySet());
        if (copy.size() == distinctCtes.size()) {
            log.debug("no CTEs there.");
            distinctCtes = vertexCluster.keySet();
        }

        List<CTEcreatorForCommonJoins.JoinCteNodeInfo> infos = prepareJoinNodeCteInfo(vertexCluster, distinctCtes);

        return infos;
    }

    @org.jetbrains.annotations.NotNull
    private static ArrayList<Integer> getIsolatedVertices(ArrayList<Integer> vertices, HashMap<Integer, HashSet<Integer>> vertexCluster) {
        ArrayList<Integer> isolatedVertices = new ArrayList<>();
        isolatedVertices.addAll(vertices);
        for (HashSet<Integer> v : vertexCluster.values()) {
            isolatedVertices.removeAll(v);
        }
        return isolatedVertices;
    }

    @org.jetbrains.annotations.NotNull
    private static List<CTEcreatorForCommonJoins.JoinCteNodeInfo> prepareJoinNodeCteInfo(HashMap<Integer, HashSet<Integer>> vertexCluster, Set<Integer> distinctCtes)
    {
        log.debug("distinct CTE nodes " + distinctCtes);
        int suffix = 0;
        List<CTEcreatorForCommonJoins.JoinCteNodeInfo> infos = new ArrayList<>();
        log.debug("vertexCluster = " + vertexCluster);
        for (int i : distinctCtes) {
            String cteName = "\"" + CTE_NODE_PREFIX + suffix++ + "\"";
            log.debug("i = " + i);
            if (vertexCluster.get(i) != null) {
                CTEcreatorForCommonJoins.JoinCteNodeInfo info = new CTEcreatorForCommonJoins.JoinCteNodeInfo(i, vertexCluster.get(i), cteName);
                infos.add(info);
            } else {
                log.debug("vertexCluster.get(i) is null");
            }
        }
        return infos;
    }

    @org.jetbrains.annotations.NotNull
    private static Set<Integer> getDistinctJoinNodesForCte(ArrayList<Integer> vertices, Map<Integer, ArrayList<Set<String>>> joinColsJoinGraphMap, Map<Integer, Integer> clusterVertexMapping) {
        Set<Integer> distinctCtes = new HashSet<>();
        Set<Integer> toRemove = new HashSet<>();

        for (int i : vertices) {
            for (int j : vertices) {
                if (i < j && joinColsJoinGraphMap.containsKey(clusterVertexMapping.get(i))
                        && joinColsJoinGraphMap.containsKey(clusterVertexMapping.get(j))) {
                    double ij_score = subGraphScore(clusterVertexMapping.get(i), clusterVertexMapping.get(j), joinColsJoinGraphMap);
                    double ji_score = subGraphScore(clusterVertexMapping.get(j), clusterVertexMapping.get(i), joinColsJoinGraphMap);

                    /**
                     * BQO based subplan merging logic
                     */
                    if (ij_score < 1 && ji_score == 1) {
                        toRemove.add(clusterVertexMapping.get(j));
                        distinctCtes.add(clusterVertexMapping.get(i));
                        log.debug("adding " + clusterVertexMapping.get(i) + " and removing " + clusterVertexMapping.get(j));
                    }
                    else if (ij_score == 1 && ji_score < 1) {
                        toRemove.add(clusterVertexMapping.get(i));
                        distinctCtes.add(clusterVertexMapping.get(j));
                        log.debug("adding " + clusterVertexMapping.get(j) + " and removing " + clusterVertexMapping.get(i));

                    }
                }
            }
        }
        distinctCtes.removeAll(toRemove);
        return distinctCtes;
    }

    @org.jetbrains.annotations.NotNull
    private static Map<Integer, Integer> createClusterVertexMapping(HashMap<Integer, HashSet<Integer>> vertexCluster, ArrayList<Integer> isolatedVertices) {
        Map<Integer, Integer> clusterVertexMapping = new HashMap<>();
        int counter = 1;
        for (Integer K : vertexCluster.keySet()) {
            clusterVertexMapping.put(counter, K);
            counter++;
        }
        for (Integer K : isolatedVertices) {
            clusterVertexMapping.put(counter, K);
            counter++;
        }
        return clusterVertexMapping;
    }

    @org.jetbrains.annotations.NotNull
    private static HashMap<Integer, HashSet<Integer>> makeVertexCluster(Map<Integer, JoinInformationExtractor.JoinInformation> joinGraphMap, ArrayList<Integer> vertices) {
        joinGraphMap.entrySet().stream().forEach(i -> vertices.add(i.getKey()));
        log.debug("Original vertices are " + vertices);

        HashMap<Integer, HashSet<Integer>> vertexCluster = new HashMap<>();
        for (int i : vertices) {
            for (int j : vertices) {
                if (i < j && joinGraphMap.containsKey(i) && joinGraphMap.containsKey(j)) {
                    log.debug(i + " vs " + j);
                    boolean ij_score = subGraphScore2(i, j, joinGraphMap);
                    boolean ji_score = subGraphScore2(j, i, joinGraphMap);
                    if (ij_score && ji_score) {
                        findAndAddToCluster(i, j, vertexCluster);
                        log.debug("common subplan");
                    }
                    else if (!ij_score && !ji_score) {
                        log.debug("no matches");
                    }
                    else {
                        log.debug("one directional match. Strange!");
                    }
                }
            }
        }
        return vertexCluster;
    }

    private static boolean subGraphScore2(int i, int j, Map<Integer, JoinInformationExtractor.JoinInformation> joinGraphMap)
    {
        JoinInformationExtractor.JoinInformation joinInfoI = joinGraphMap.get(i);
        JoinInformationExtractor.JoinInformation joinInfoJ = joinGraphMap.get(j);
        return joinInfoI.equals(joinInfoJ);
    }

    private static void findAndAddToCluster(int i, int j, HashMap<Integer, HashSet<Integer>> vertexCluster)
    {
        for (HashSet<Integer> ver : vertexCluster.values()) {
            if (ver.contains(i) || ver.contains(j)) {
                ver.add(i);
                ver.add(j);
                return;
            }
        }
        HashSet<Integer> cluster = new HashSet<>();
        cluster.add(i);
        cluster.add(j);
        vertexCluster.put(i, cluster);
    }

    private static double subGraphScore(int i, int j, Map<Integer, ArrayList<Set<String>>> processedJoinGraphMap)
    {
        ArrayList<Set<String>> joinListI = processedJoinGraphMap.get(i);
        ArrayList<Set<String>> joinListJ = processedJoinGraphMap.get(j);

        Set<Set<String>> common = joinListI.stream()
                .distinct()
                .filter(joinListJ::contains)
                .collect(Collectors.toSet());

        double score = common.size() * 1.0 / joinListI.size();

        log.debug("i = " + i + " j = " + j + " score = " + score + " (subset count) = " + common.size() + " Denominator count = " + joinListI.size());
        return score;
    }
}