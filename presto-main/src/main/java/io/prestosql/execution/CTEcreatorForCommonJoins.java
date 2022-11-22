package io.prestosql.execution;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class CTEcreatorForCommonJoins
{
    private static final Logger log = Logger.get(CTEcreatorForCommonJoins.class);

    public static final String CTE_NODE_FOUND_FILENAME = "cte_added_for.txt";

    private final PlanNode root;
    private final PlanNodeIdAllocator idAllocator;
    private final List<JoinCteNodeInfo> joinIdxToCteNameMap;

    public CTEcreatorForCommonJoins(PlanNode root, PlanNodeIdAllocator idAllocator, List<JoinCteNodeInfo> map)
    {
        this.root = root;
        this.idAllocator = idAllocator;
        this.joinIdxToCteNameMap = requireNonNull(map, "Join CTE node info should not be null");
    }

    public PlanNode addCTE()
    {
        return root.accept(new CteNodeAdder(this.joinIdxToCteNameMap, this.idAllocator), null);
    }

    public static class JoinCteNodeInfo
    {
        final int clusterId;
        final Set<Integer> vertices;
        final String cteName;

        public JoinCteNodeInfo(int key, Set<Integer> vertices, String name)
        {
            this.clusterId = key;
            this.vertices = vertices;
            this.cteName = name;
        }

        public int getCommonCteRefNum()
        {
            return clusterId;
        }
    }

    private static class CteNodeAdder extends InternalPlanVisitor<PlanNode, Void>
    {
        private int joinCount;
        private final PlanNodeIdAllocator idAllocator;
        private final List<Integer> joinCtes;
        private final Map<Integer, Integer> vertexClusterMap;
        private final Map<Integer, JoinCteNodeInfo> map;

        private static void logCTEInfoToFile(Set<Integer> ctes)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\n");
            for (Integer i : ctes) {
                sb.append(i).append(" ");
            }

            try {
                Files.write(Paths.get(CTE_NODE_FOUND_FILENAME), sb.toString().getBytes(), StandardOpenOption.APPEND);
            } catch (IOException e) {
                log.debug("some file error");
            }
        }

        public CteNodeAdder(List<JoinCteNodeInfo> map, PlanNodeIdAllocator idAllocator)
        {
            this.joinCtes = new ArrayList<>();
            this.vertexClusterMap = new HashMap<>();
            this.map = new HashMap<>();
            this.joinCount = 0;
            this.idAllocator = idAllocator;
            for (JoinCteNodeInfo i : map) {
                joinCtes.addAll(i.vertices);
                for (Integer iv : i.vertices) {
                    this.vertexClusterMap.put(iv, i.clusterId);
                    logCTEInfoToFile(i.vertices);
                    logCTEInfoToFile(ImmutableSet.of());
                }
                this.map.put(i.clusterId, i);
            }
            log.debug("join node indexs for cte " + joinCtes);
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            Set<PlanNodeId> dummy = new HashSet<>();
            List<PlanNode> newSources = new ArrayList<>();
            for (PlanNode src : node.getSources()) {
                src = src.accept(this, context);
                CTEScanNode parent = null;
                if (src instanceof JoinNode) {
                    JoinNode join = (JoinNode) src;
                    if (!join.getCriteria().isEmpty()) {
                        this.joinCount++;
                        log.debug("join count " + joinCount);
                        if (this.joinCtes.contains(joinCount)) {
                            int i = this.vertexClusterMap.get(joinCount);
                            JoinCteNodeInfo info = this.map.get(i);
                            parent = new CTEScanNode(idAllocator.getNextId(), join,
                                    join.getOutputSymbols(), Optional.empty(), info.cteName, dummy, info.getCommonCteRefNum());
                            newSources.add(parent);
                            log.debug("cte added");
                        }
                    }
                }
                if (parent == null) {
                    newSources.add(src);
                }
            }
            return node.replaceChildren(newSources);
        }
    }
}
