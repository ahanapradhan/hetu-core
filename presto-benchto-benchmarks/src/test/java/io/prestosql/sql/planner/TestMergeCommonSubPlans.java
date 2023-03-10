package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.tpcds.TpcdsConnectorFactory;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestMergeCommonSubPlans extends BasePlanTest
{
    public TestMergeCommonSubPlans(ImmutableMap<String, String> sessionProperties)
    {
        super(sessionProperties);
    }

    @Override
    protected LocalQueryRunner createQueryRunner()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1");
             //   .setSystemProperty("enable-dynamic-filtering","true");// these tests don't handle exchanges from local parallel

        sessionProperties.entrySet().forEach(entry -> sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue()));

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(sessionBuilder.build());

        localQueryRunner.createCatalog(localQueryRunner.getDefaultSession().getCatalog().get(),
                new TpcdsConnectorFactory(1),
                ImmutableMap.of());
        return localQueryRunner;
    }

    public Map<String, Integer> getCteCounter(String sql)
    {
        PlanNode node = plan(sql).getRoot();
        HashScorePrinter hashCounter = new HashScorePrinter();
        node.accept(hashCounter, 0);
        return hashCounter.getCteCounter();
    }

    public int getProjectionCount(String sql)
    {
        PlanNode node = plan(sql).getRoot();
        ProjectionCounter pc = new ProjectionCounter();
        node.accept(pc, null);
        return pc.getProjectionCount();
    }

    private static class ProjectionCounter extends InternalPlanVisitor<Void, Integer>
    {
        int projectionCount;
        @Override
        public Void visitPlan(PlanNode node, Integer context)
        {
            if (node instanceof ProjectNode) {
                projectionCount++;
            }
            for (PlanNode source : node.getSources()) {
                source.accept(this, null);
            }
            return null;
        }

        public int getProjectionCount()
        {
            return projectionCount;
        }
    }

    public static class HashScorePrinter extends InternalPlanVisitor<Void, Integer>
    {
        private final Map<String, Integer> cteCounter = new HashMap<>();

        public Map<String, Integer> getCteCounter()
        {
            return this.cteCounter;
        }

        private String getDetails(PlanNode node)
        {
            String nodeType = node.getClass().toString().replace("class io.prestosql.spi.plan.","").replace("class io.prestosql.sql.planner.plan.","");
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
            if (node instanceof UnionNode) {
                sb.append(node.getOutputSymbols());
            }
            sb.append(" ").append(node.getOutputSymbols());
            return sb.toString();
        }
        @Override
        public Void visitPlan(PlanNode node, Integer context)
        {
            updateCteCounter(node);

            IntStream.range(0, context).boxed().forEach(i -> System.out.print("\t"));
            System.out.println(getDetails(node) + " " + node.getHash());
            for (PlanNode source : node.getSources()) {
                source.accept(this, context + 1);
            }
            return null;
        }

        private void updateCteCounter(PlanNode node) {
            if (node instanceof CTEScanNode) {
                String cteName = ((CTEScanNode) node).getCteRefName();
                if (this.cteCounter.containsKey(cteName)) {
                    this.cteCounter.put(cteName, cteCounter.get(cteName) + 1);
                }
                else {
                    this.cteCounter.put(cteName, 1);
                }
            }
        }
    }

    @Test
    public void test_hash()
    {
        System.out.println(Objects.hash(true, false) + " " + Objects.hash(false, true));
    }


    @Test
    public void testScore_1()
    {
        String query = "select d_year from date_dim d, store_sales ss1, store_sales ss2 where ss1.ss_sold_date_sk = d.d_date_sk and ss2.ss_sold_date_sk = d.d_date_sk";
        Plan queryPlan = plan(query);
        System.out.println(queryPlan.getRoot());
        queryPlan.getRoot().accept(new HashScorePrinter(), 0);
    }

    @Test
    public void testScore_3()
    {
        String query = "select d_year from date_dim d, store_sales ssone, store_sales sstwo where ssone.ss_sold_date_sk = d.d_date_sk and sstwo.ss_sold_date_sk = d.d_date_sk and ssone.ss_quantity > 10 and sstwo.ss_quantity = ssone.ss_quantity";
        Plan queryPlan = plan(query);
        System.out.println(queryPlan.getRoot());
        queryPlan.getRoot().accept(new HashScorePrinter(), 0);
    }

    @Test
    public void testScore_2()
    {
        String query = "SELECT c_customer_sk, \n" +
                "                        Sum(ss_quantity * ss_sales_price) csales \n" +
                "                 FROM   store_sales, \n" +
                "                        customer, \n" +
                "                        date_dim \n" +
                "                 WHERE  ss_customer_sk = c_customer_sk \n" +
                "                        AND ss_sold_date_sk = d_date_sk \n" +
                "                        AND d_year IN ( 1998, 1998 + 1, 1998 + 2, 1998 + 3 ) \n" +
                "                 GROUP  BY c_customer_sk";
        Plan queryPlan = plan(query);
        System.out.println(queryPlan.getRoot());
        queryPlan.getRoot().accept(new HashScorePrinter(), 0);
    }

    @Test
    public void testScore_4()
    {
        String query = "WITH best_ss_customer AS " +
                "(SELECT c_customer_sk, \n" +
                "Sum(ss_quantity * ss_sales_price) ssales \n" +
                "FROM   store_sales, \n" +
                "customer \n" +
                "WHERE  ss_customer_sk = c_customer_sk \n" +
                "GROUP  BY c_customer_sk \n" +
                "HAVING Sum(ss_quantity) > ( 95 / 100.0 ))\n" +
                "SELECT Sum(sales) \n" +
                "FROM   (SELECT cs_quantity * cs_list_price sales \n" +
                "FROM   catalog_sales, \n" +
                "date_dim \n" +
                "WHERE  d_year = 1998 \n" +
                "AND d_moy = 6 \n" +
                "AND cs_sold_date_sk = d_date_sk \n" +
                "AND cs_bill_customer_sk IN (SELECT c_customer_sk \n" +
                "FROM   best_ss_customer) \n" +
                "UNION ALL \n" +
                "SELECT ws_quantity * ws_list_price sales \n" +
                "FROM   web_sales, \n" +
                "date_dim \n" +
                "WHERE  d_year = 1998 \n" +
                "AND d_moy = 6 \n" +
                "AND ws_sold_date_sk = d_date_sk \n" +
                "AND ws_bill_customer_sk IN (SELECT c_customer_sk \n" +
                "FROM   best_ss_customer)) \n" +
                "LIMIT 100";
        Plan queryPlan = plan(query);
        System.out.println(queryPlan.getRoot());
        queryPlan.getRoot().accept(new HashScorePrinter(), 0);
    }

    public MaterializedResult executeSql(String sql)
    {
        return queryRunner.execute(sql);
    }
}

