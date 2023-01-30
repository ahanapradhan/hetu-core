package io.prestosql.sql.planner.optimizations;

import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.CteNodeRemover;
import io.prestosql.execution.HashComputerForPlanTree;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import org.apache.commons.lang3.time.StopWatch;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class MergeCommonSubPlans implements PlanOptimizer
{
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

     /*   System.out.println("original plan--start------------------------------");
        plan.accept(new HashScorePrinter(), 0);
        System.out.println("original plan--end------------------------------");
*/
        HashComputerForPlanTree hashComputer = new HashComputerForPlanTree(plan);
        hashComputer.computeHash();

        plan = introduceCTEs(plan, idAllocator, hashComputer);
     /*   System.out.println("new plan--start------------------------------");
        plan.accept(new HashScorePrinter(), 0);
        System.out.println("new plan--end------------------------------");
*/
        watch.stop();
        log.debug("total time: " + watch.getTime(TimeUnit.MILLISECONDS) + " ms");

        return plan;
    }

    private static class HashScorePrinter extends InternalPlanVisitor<Void, Integer>
    {
        private String getDetails(PlanNode node) {
            String nodeType = node.getClass().toString().replace("class io.prestosql.spi.plan.", "").replace("class io.prestosql.sql.planner.plan.", "");
            StringBuilder sb = new StringBuilder();
            sb.append(nodeType).append(" ");
            if (node instanceof TableScanNode) {
                sb.append(((TableScanNode) node).getTable().getFullyQualifiedName());
            }
            if (node instanceof FilterNode) {
                sb.append(((FilterNode) node).getPredicate().toString());
            }
            if (node instanceof ProjectNode) {
                sb.append(((ProjectNode) node).getAssignments().getExpressions());
            }
            if (node instanceof AggregationNode) {
                sb.append(((AggregationNode) node).getAggregations());
            }
            sb.append(" ").append(node.getOutputSymbols());
            return sb.toString();
        }

        @Override
        public Void visitPlan(PlanNode node, Integer context)
        {
            IntStream.range(0, context).boxed().forEach(i -> System.out.print("\t"));
            System.out.println(getDetails(node) + " " + node.getHash());
            for (PlanNode source : node.getSources()) {
                source.accept(this, context + 1);
            }
            return null;
        }
    }


        /**
     * BFS order traversal and introduce CTE nodes
     */
    private PlanNode introduceCTEs(PlanNode root, PlanNodeIdAllocator idAllocator, HashComputerForPlanTree hashCounter) {
        TopDownTraverserForAddCTE traverser = new TopDownTraverserForAddCTE(root, hashCounter, idAllocator);
        traverser.topDownCTEAdd();
        return traverser.root;
    }


    private static class TopDownTraverserForAddCTE {
        private static int CTE_COUNTER = 0;
        private static final String CTE_PREFIX = "$CTE_Node_";
        private final HashComputerForPlanTree hashCounter;
        private PlanNode root;
        private final PlanNodeIdAllocator idAllocator;

        public TopDownTraverserForAddCTE(PlanNode root, HashComputerForPlanTree hashCounter, PlanNodeIdAllocator idAllocator) {
            this.hashCounter = hashCounter;
            this.root = root;
            this.idAllocator = idAllocator;
        }

        private static String getUniqueCtePrefix() {
            return CTE_PREFIX + CTE_COUNTER++;
        }

        private static int getNextCommonCteRefNum() {
            Random rand = new Random();
            return rand.nextInt();
        }

        public void topDownCTEAdd() {
            Map<Integer, String> ctePrefixMap = new HashMap<>();
            Map<Integer, Integer> cteCommonRefNumMap = new HashMap<>();
            HashComputerForPlanTree.HashStats hashes = hashCounter.collectHash(this.root);

            for (Integer key : hashes.hashCounter.keySet()) {
                if (hashes.hashCounter.get(key) > 1) {
                    ctePrefixMap.put(key, getUniqueCtePrefix());
                    cteCommonRefNumMap.put(key, getNextCommonCteRefNum());
                }
            }

            ctePrefixMap.keySet().stream().forEach(key -> log.debug(key + ": " + ctePrefixMap.get(key)));

            CTENodeAdder cteAdder = new CTENodeAdder(idAllocator, ctePrefixMap, cteCommonRefNumMap, hashes);
            this.root = cteAdder.visitPlan(this.root, null);
        }
    }

    private static class CTENodeAdder extends InternalPlanVisitor<PlanNode, Void> {

        private final PlanNodeIdAllocator idAllocator;
        private final Map<Integer, String> ctePrifxMap;
        private final Map<Integer, Integer> commonCteRefNum;
        private final HashComputerForPlanTree.HashStats hashStats;

        public CTENodeAdder(PlanNodeIdAllocator idAllocator, Map<Integer, String> ctePrefixMap, Map<Integer, Integer> cteCommonRefNumMap, HashComputerForPlanTree.HashStats stats) {
            this.idAllocator = idAllocator;
            this.commonCteRefNum = cteCommonRefNumMap;
            this.ctePrifxMap = ctePrefixMap;
            this.hashStats = stats;
        }

        private boolean isNotScanFilterProject(PlanNode node)
        {
            if (node.getSources().size() > 1) {
                return true;
            }
            if (!(node instanceof TableScanNode) && !(node instanceof FilterNode) && !(node instanceof ProjectNode)) {
                return true;
            }
            if (node instanceof TableScanNode) {
                return false;
            }
            if (node instanceof FilterNode && (node.getSources().get(0) instanceof TableScanNode)) {
                return false;
            }
            return !(node instanceof ProjectNode) || !(node.getSources().get(0) instanceof FilterNode) || !(node.getSources().get(0).getSources().get(0) instanceof TableScanNode);
        }


        @Override
        public PlanNode visitPlan(PlanNode node, Void context) {
            List<PlanNode> reWrittenSources = new ArrayList<>();
            for (PlanNode source : node.getSources()) {
                source = source.accept(this, null);
                Integer hash = source.getHash();
                if (ctePrifxMap.containsKey(hash)) {
                    source = source.accept(new RedundantCTERemover(hashStats, ctePrifxMap), null);
                    if (isNotScanFilterProject(source)) {
                        String ctePrefix = ctePrifxMap.get(hash);
                        int commonRefNum = commonCteRefNum.get(hash);
                        PlanNode parentCte = new CTEScanNode(idAllocator.getNextId(), source, source.getOutputSymbols(), Optional.empty(), ctePrefix, new HashSet<>(), commonRefNum);
                        log.debug("cte node created with name " + ctePrefix + ", commonrefNum: " + commonRefNum);
                        reWrittenSources.add(parentCte);
                    }
                    else {
                        reWrittenSources.add(source);
                    }
                } else {
                    reWrittenSources.add(source);
                }
            }
            if (!reWrittenSources.isEmpty()) {
                log.debug("cte node to be added after " + node);
                node = node.replaceChildrenWithHash(reWrittenSources);
            }
            return node;
        }

    }

    private static class RedundantCTERemover extends InternalPlanVisitor<PlanNode, Void>
    {
        private final HashComputerForPlanTree.HashStats hashStats;
        private final Map<Integer, String> ctePrifxMap;
        private int lastCTEfreq;

        public RedundantCTERemover(HashComputerForPlanTree.HashStats hashStats, Map<Integer, String> ctePrifxMap)
        {
            this.hashStats = hashStats;
            this.ctePrifxMap = ctePrifxMap;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            if (node instanceof CTEScanNode) {
              /*  int hash = ((CTEScanNode) node).getSource().getHash();
                lastCTEfreq = this.hashStats.hashCounter.get(hash);*/
                return ((CTEScanNode) node).getSource();
            }
            if (node.getSources().isEmpty()) {
                return node;
            }

            PlanNode node1 = handlePlanBranches(node);
            if (node1 != null) {
                return node1;
            }

            List<PlanNode> reWrittenSources = new ArrayList<>();

            for (PlanNode source : node.getSources()) {
               /* if (source instanceof CTEScanNode) {
                    PlanNode child = ((CTEScanNode) source).getSource();
                    reWrittenSources.add(child);
                } else {*/
                reWrittenSources.add(source.accept(this, null));
               // }
            }
            return node.replaceChildren(reWrittenSources);
        }

        private boolean isCTEAboveShouldRemoveScan(CTEScanNode node)
        {
            PlanNode scanNode = node.getSource();
            int scanFreq = this.hashStats.hashCounter.get(scanNode.getHash());
            return  (scanNode instanceof TableScanNode) && (scanFreq > lastCTEfreq);
        }

        @Nullable
        private PlanNode handlePlanBranches(PlanNode node)
        {
            if (node.getSources().size() > 1) { // e.g. join node
                Set<Set<String>> names = new HashSet<>();
                for (PlanNode src : node.getSources()) {
                    Set<String> cteNames = src.accept(new CTENameCollector(), null);
                    names.add(cteNames);
                }
                if (hasOverLap(names)) {
                    return node;
                }
            }
            return null;
        }

        private boolean hasOverLap(Set<Set<String>> names)
        {
            boolean flag = false;
            int ctx = 0, cty;
            for (Set<String> ctex : names) {
                cty = 0;
                for (Set<String> ctey: names) {
                    if (ctx != cty) {
                        Set<String> copy = new HashSet<>(ctex);
                        copy.removeAll(ctey);
                        flag = flag || (copy.size() != ctex.size());
                    }
                    cty++;
                }
                ctx++;
            }
            return flag;
        }

        private boolean isCTEAboveNonScan(CTEScanNode cte)
        {
            PlanNode scanNode = cte.getSource();
            return scanNode instanceof TableScanNode;
        }
    }

    private static class CTENameCollector extends InternalPlanVisitor<Set<String>, Void> {

        @Override
        public Set<String> visitPlan(PlanNode node, Void context) {
            Set<String> names = new HashSet<>();
            if (node instanceof CTEScanNode) {
                names.add(((CTEScanNode) node).getCteRefName());
            }
            for (PlanNode source : node.getSources()) {
                names.addAll(source.accept(this, null));
            }
            return names;
        }
    }
}

