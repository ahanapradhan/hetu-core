package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.TreeTraverser;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.CteNodeRemover;
import io.prestosql.execution.HashComputerForPlanTree;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MergeCommonSubPlans implements PlanOptimizer {
    private static final Logger log = Logger.get(MergeCommonSubPlans.class);

    @Override
    public PlanNode optimize(PlanNode plan,
                             Session session,
                             TypeProvider types,
                             PlanSymbolAllocator planSymbolAllocator,
                             PlanNodeIdAllocator idAllocator,
                             WarningCollector warningCollector) {
        if (!SystemSessionProperties.isSubplanMergeEnabled(session)) {
            return plan;
        }

        StopWatch watch = new StopWatch("Global Planner for CTE");
        watch.start();

        CteNodeRemover cteRemover = new CteNodeRemover(plan);
        plan = cteRemover.removeCTE();

        HashComputerForPlanTree hashComputer = new HashComputerForPlanTree(plan);
        hashComputer.computeHash();

        plan = introduceCTEs(plan, idAllocator, hashComputer);

        watch.stop();
        log.debug("total time: " + watch.getTime(TimeUnit.MILLISECONDS) + " ms");

        return plan;
    }

    /**
     * BFS order traversal and introduce CTE nodes
     */
    private PlanNode introduceCTEs(PlanNode root, PlanNodeIdAllocator idAllocator, HashComputerForPlanTree hashCounter) {
        TopDownBFSTraverserForAddCTE traverser = new TopDownBFSTraverserForAddCTE(root, hashCounter, idAllocator);
        traverser.topDownCTEAdd();
        return traverser.root;
    }


    private static class TopDownBFSTraverserForAddCTE {
        private final HashComputerForPlanTree hashCounter;
        private PlanNode root;
        private final PlanNodeIdAllocator idAllocator;

        public TopDownBFSTraverserForAddCTE(PlanNode root, HashComputerForPlanTree hashCounter, PlanNodeIdAllocator idAllocator) {
            this.hashCounter = hashCounter;
            this.root = root;
            this.idAllocator = idAllocator;
        }

        private PlanNode addCTENode(int key, List<PlanNode> nodes, PlanNode root) {
            CTENodeAdder cteAdder = new CTENodeAdder(key, nodes, idAllocator);
            return root.accept(cteAdder, null);
        }

        public void topDownCTEAdd()
        {
            Set<Integer> pastKeys = new HashSet<>();
            HashComputerForPlanTree.HashStats hashes = hashCounter.collectHash(this.root);
            Set<Integer> probablyCteKeys = new HashSet<>();
            for (Integer key : hashes.hashCounter.keySet()) {
                if (hashes.hashCounter.get(key) > 1) {
                    probablyCteKeys.add(key);
                }
            }

            TreeTraverser<PlanNode> traverser = TreeTraverser.using(n -> n.getSources());
            for (PlanNode node : traverser.breadthFirstTraversal(this.root)) {
                log.debug("visiting node: " + node);
                Integer key = node.getHash();
                if (probablyCteKeys.contains(key) && !pastKeys.contains(key)) {
                    log.debug(" should be a CTE");
                    this.root = addCTENode(key, (List<PlanNode>) hashes.hashKeyToParentsMap.get(key), this.root);
                    log.debug("added CTE for hash key " + key);
                }
                pastKeys.add(key);
            }
        }
    }


    private static class CTENodeAdder extends InternalPlanVisitor<PlanNode, Void> {
        private final int childHash;
        private final List<PlanNode> parents;
        private final String cteRefName;
        private static int uniq = 0;
        private static final String CTE_PREFIX = "$CTE_Node_";
        private final PlanNodeIdAllocator idAllocator;
        private final int commonCteRefNum;

        public CTENodeAdder(int childHash, Collection<PlanNode> parents, PlanNodeIdAllocator idAllocator) {
            this.childHash = childHash;
            this.parents = (List<PlanNode>) parents;
            this.cteRefName = "\"" + CTE_PREFIX + uniq++ + "\"";
            this.idAllocator = idAllocator;
            this.commonCteRefNum = getRandInt();
        }

        private int getRandInt() {
            Random rand = new Random();
            return rand.nextInt();
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context) {
            if (this.parents.contains(node)) {
                Set<PlanNode> ctes = new HashSet<>();
                Set<PlanNode> matches = node.getSources().stream().filter(s -> (s.getHash() == this.childHash)).collect(Collectors.toSet());

                for (PlanNode child : matches) {
                    PlanNode cte = new CTEScanNode(idAllocator.getNextId(), child,
                            child.getOutputSymbols(), Optional.empty(), cteRefName, new HashSet<>(), this.commonCteRefNum);
                    //  log.debug("cte node " + cteRefName + " got added");
                    ctes.add(cte);
                }
                List<PlanNode> children = new ArrayList<>();
                children.addAll(node.getSources());
                children.removeAll(matches);
                children.addAll(ctes);
                node = node.replaceChildrenWithHash(children);
            } else {
                List<PlanNode> newSources = new ArrayList<>();
                for (PlanNode src : node.getSources()) {
                    newSources.add(src.accept(this, null));
                }
                node = node.replaceChildrenWithHash(newSources);
            }
            return node;
        }
    }
}

