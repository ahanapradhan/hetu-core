package io.prestosql.sql.planner.optimizations;

import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cost.CachingCostProvider;
import io.prestosql.cost.CachingStatsProvider;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.CostProvider;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cost.StatsProvider;
import io.prestosql.execution.CteNodeRemover;
import io.prestosql.execution.HashComputerForPlanTree;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.datapath.globalplanner.GlobalPlanner;
import io.prestosql.sql.planner.datapath.globalplanner.JoinInformationExtractor;
import org.apache.commons.lang3.time.StopWatch;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MergeCommonJoins implements PlanOptimizer
{
    private static final Logger log = Logger.get(MergeCommonJoins.class);

    private final AtomicInteger currentBatchSize = new AtomicInteger(0);
    private final Map<Integer, JoinInformationExtractor.JoinInformation> joinGraphMap = new HashMap<>();

    private GlobalPlanner globalPlaner = null;
    private PlanNodeIdAllocator idAllocator = null;
    private PlanSymbolAllocator planSymbolAllocator = null;
    private TypeProvider types = null;
    private Session session = null;

    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    public MergeCommonJoins(StatsCalculator statsCalculator,
                            CostCalculator costCalculator)
    {
        this.statsCalculator = statsCalculator;
        this.costCalculator = costCalculator;
    }

    private StatsAndCosts calculatePlanCost(PlanNode root)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types);
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session, types);
        StatsAndCosts wholePlanCost = StatsAndCosts.create(root, statsProvider, costProvider);
        return wholePlanCost;
    }

    private void initialize(PlanNodeIdAllocator idAllocator,
            PlanSymbolAllocator planSymbolAllocator,
            TypeProvider types,
            Session session)
    {
        this.idAllocator = idAllocator;
        this.planSymbolAllocator = planSymbolAllocator;
        this.types = types;
        this.session = session;
        this.globalPlaner = new GlobalPlanner();
        this.joinGraphMap.clear();
        this.currentBatchSize.set(0);
    }

    @Override
    public PlanNode optimize(PlanNode plan,
                             Session session,
                             TypeProvider types,
                             PlanSymbolAllocator planSymbolAllocator,
                             PlanNodeIdAllocator idAllocator,
                             WarningCollector warningCollector)
    {
        if (!SystemSessionProperties.isSubplanMergeEnabled(session)) {
            return plan;
        }

        initialize(idAllocator, planSymbolAllocator, types, session);

        StatsAndCosts wholePlanCost = calculatePlanCost(plan);

        StopWatch watch = new StopWatch("Global Planner for CTE");
        watch.start();

        CteNodeRemover cteRemover = new CteNodeRemover(plan);
        plan = cteRemover.removeCTE();

        HashComputerForPlanTree hashComputer = new HashComputerForPlanTree(plan);
        Map<Integer, Set<PlanNode>> hashCounter = hashComputer.computeHash();

        for (Map.Entry<Integer, Set<PlanNode>> map : hashCounter.entrySet()) {
            Set<PlanNode> dup = map.getValue();
            if (dup.size() > 1) {
                log.debug("duplicates:");
                for (PlanNode node : dup) {
                    log.debug(node.toString());
                }
                log.debug("-------------------");
            }
        }

        /**
         *

        JoinNodePicker splitter = new JoinNodePicker(plan);
        List<JoinNode> joinNodes = splitter.splitAtJoins();
        log.debug("number of join rooted trees : " + joinNodes.size());

        for (JoinNode join : joinNodes) {

            Plan plan2 = new Plan(join, types, wholePlanCost.getForSubplan(join));
            currentBatchSize.getAndAdd(1);

            JoinInformationExtractor.JoinInformation joinInformation = globalPlaner.getJoinInfoQueryOrder(plan2.getRoot(), plan2.getStatsAndCosts());
            logJoinInfo(joinInformation);
            joinGraphMap.put(currentBatchSize.intValue(), joinInformation);
        }

        List<CTEcreatorForCommonJoins.JoinCteNodeInfo> infos = globalPlaner.createOverlapMatches(joinGraphMap);
        CTEcreatorForCommonJoins adder = new CTEcreatorForCommonJoins(plan, idAllocator, infos);
        plan = adder.addCTE();

         */

        watch.stop();
        log.debug("total time: " + watch.getTime(TimeUnit.MILLISECONDS) + " ms");

        return plan;
    }

    private void logJoinInfo(JoinInformationExtractor.JoinInformation joinInformation) {
        log.debug(currentBatchSize.intValue() + " join info log start------------------------------");
        for (JoinInformationExtractor.JoinNodeInfo info : joinInformation.joinNodesInfo) {
            log.debug(info.equiJoinClause.toString());
        }
        for (Map.Entry<String, JoinInformationExtractor.ScanNodeInfo> e : joinInformation.tableNameToScanNodesInfo.entrySet()) {
            log.debug("Table: " + e.getKey() + ", output symbols: " + e.getValue().scanNodeRef.getOutputSymbols());
        }
        for (Map.Entry<String, JoinInformationExtractor.FilterNodeInfo> e : joinInformation.tableNameToFilterNodesInfo.entrySet()) {
            log.debug("Table: " + e.getKey() + ", output symbols: " + e.getValue().filterNodeRef.getOutputSymbols());
        }
        log.debug("join info log end------------------------------");
    }

}
