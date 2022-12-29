package io.prestosql.execution;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;

import java.util.HashMap;
import java.util.Map;

public class HashComputerForPlanTree
{
    private final PlanNode root;
    private static final Logger log = Logger.get(HashComputerForPlanTree.class);

    public HashComputerForPlanTree(PlanNode root)
    {
        this.root = root;
    }

    public Void computeHash()
    {
        return root.accept(new HashComputeVisitor(), null);
    }

    public HashStats collectHash(PlanNode node)
    {
        HashCollectVisitor hashCollector = new HashCollectVisitor();
        node.accept(hashCollector, null);
        return new HashStats(hashCollector.hashKeyToParentsMap, hashCollector.hashCounter);
    }

    /**
     *     visits plan nodes to compute hash for each node.
     *     Bottom up hash compute. As a node needs elements from subtree to compute own hash
     */

    private static class HashComputeVisitor extends InternalPlanVisitor<Void, Long>
    {
        @Override
        public Void visitPlan(PlanNode node, Long context)
        {
            for (PlanNode s : node.getSources()) {
                s.accept(this, context);
            }
            node.getHash();
          //  log.debug("node: " + node + ", hash: " + hash);
            return null;
        }
    }

    public static class HashStats
    {
        public final Multimap<Integer, PlanNode> hashKeyToParentsMap;
        public final Map<Integer, Integer> hashCounter;

        public HashStats(Multimap<Integer, PlanNode> map, Map<Integer, Integer> hashCounter)
        {
            this.hashCounter = hashCounter;
            this.hashKeyToParentsMap = map;
        }
    }

    /**
     * visits plan nodes to look for matching in subtrees (by looking at hash value).
     * Top-down matching, as outer matching should first converted to CTE in order not to generate redundant CTE nodes
     *
     * This traversal is in DFS order
     */

    private static class HashCollectVisitor extends InternalPlanVisitor<Void, Void>
    {
        Multimap<Integer, PlanNode> hashKeyToParentsMap = ArrayListMultimap.create();
        Map<Integer, Integer> hashCounter = new HashMap<>();
        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode s : node.getSources()) {
                Integer key = s.getHash();
                hashKeyToParentsMap.put(key, node); // child hash --> parent node. This way, all the nodes having same subtree below will be grouped together
                if (!hashCounter.containsKey(key)) {
                    hashCounter.put(key, 1);
                }
                else {
                    hashCounter.put(key, hashCounter.get(key) + 1);
                }
                s.accept(this, null);
            }
            return null;
        }
    }
}
