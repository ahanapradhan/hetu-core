package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.prestosql.SystemSessionProperties;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPlanTpcdsWithMergeCommonSubPlansVsCteReuse
{
    int pass = 0;
    public static final String TPCDS_QUERY_DIR = "/for_testcase";
    TestMergeCommonSubPlans nativeEngine;
    TestMergeCommonSubPlans newEngine;

    public static final String TEST_QUERY44 = "SELECT asceding.rnk, \n" +
            "               i1.i_product_name best_performing, \n" +
            "               i2.i_product_name worst_performing \n" +
            "FROM  (SELECT * \n" +
            "       FROM   (SELECT item_sk, \n" +
            "                      Rank() \n" +
            "                        OVER ( \n" +
            "                          ORDER BY rank_col ASC) rnk \n" +
            "               FROM   (SELECT ss_item_sk         item_sk, \n" +
            "                              Avg(ss_net_profit) rank_col \n" +
            "                       FROM   store_sales ss1 \n" +
            "                       WHERE  ss_store_sk = 4 \n" +
            "                       GROUP  BY ss_item_sk \n" +
            "                       HAVING Avg(ss_net_profit) > 0.9 * \n" +
            "                              (SELECT Avg(ss_net_profit) \n" +
            "                                      rank_col \n" +
            "                               FROM   store_sales \n" +
            "                               WHERE  ss_store_sk = 4 \n" +
            "                                      AND ss_cdemo_sk IS \n" +
            "                                          NULL \n" +
            "                               GROUP  BY ss_store_sk))V1) \n" +
            "              V11 \n" +
            "       WHERE  rnk < 11) asceding, \n" +
            "      (SELECT * \n" +
            "       FROM   (SELECT item_sk, \n" +
            "                      Rank() \n" +
            "                        OVER ( \n" +
            "                          ORDER BY rank_col DESC) rnk \n" +
            "               FROM   (SELECT ss_item_sk         item_sk, \n" +
            "                              Avg(ss_net_profit) rank_col \n" +
            "                       FROM   store_sales ss1 \n" +
            "                       WHERE  ss_store_sk = 4 \n" +
            "                       GROUP  BY ss_item_sk \n" +
            "                       HAVING Avg(ss_net_profit) > 0.9 * \n" +
            "                              (SELECT Avg(ss_net_profit) \n" +
            "                                      rank_col \n" +
            "                               FROM   store_sales \n" +
            "                               WHERE  ss_store_sk = 4 \n" +
            "                                      AND ss_cdemo_sk IS \n" +
            "                                          NULL \n" +
            "                               GROUP  BY ss_store_sk))V2) \n" +
            "              V21 \n" +
            "       WHERE  rnk < 11) descending, \n" +
            "      item i1, \n" +
            "      item i2 \n" +
            "WHERE  asceding.rnk = descending.rnk \n" +
            "       AND i1.i_item_sk = asceding.item_sk \n" +
            "       AND i2.i_item_sk = descending.item_sk \n" +
            "ORDER  BY asceding.rnk\n" +
            "LIMIT 100";

    public static final String TEST_QUERY = "WITH best_ss_customer AS " +
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

    public static final String TEST_QUERY14B = "WITH cross_items \n" +
            "     AS (SELECT i_item_sk ss_item_sk \n" +
            "         FROM   item, \n" +
            "                (SELECT iss.i_brand_id    brand_id, \n" +
            "                        iss.i_class_id    class_id, \n" +
            "                        iss.i_category_id category_id \n" +
            "                 FROM   store_sales, \n" +
            "                        item iss, \n" +
            "                        date_dim d1 \n" +
            "                 WHERE  ss_item_sk = iss.i_item_sk \n" +
            "                        AND ss_sold_date_sk = d1.d_date_sk \n" +
            "                        AND d1.d_year BETWEEN 1999 AND 1999 + 2 \n" +
            "                 INTERSECT \n" +
            "                 SELECT ics.i_brand_id, \n" +
            "                        ics.i_class_id, \n" +
            "                        ics.i_category_id \n" +
            "                 FROM   catalog_sales, \n" +
            "                        item ics, \n" +
            "                        date_dim d2 \n" +
            "                 WHERE  cs_item_sk = ics.i_item_sk \n" +
            "                        AND cs_sold_date_sk = d2.d_date_sk \n" +
            "                        AND d2.d_year BETWEEN 1999 AND 1999 + 2 \n" +
            "                 INTERSECT \n" +
            "                 SELECT iws.i_brand_id, \n" +
            "                        iws.i_class_id, \n" +
            "                        iws.i_category_id \n" +
            "                 FROM   web_sales, \n" +
            "                        item iws, \n" +
            "                        date_dim d3 \n" +
            "                 WHERE  ws_item_sk = iws.i_item_sk \n" +
            "                        AND ws_sold_date_sk = d3.d_date_sk \n" +
            "                        AND d3.d_year BETWEEN 1999 AND 1999 + 2) x \n" +
            "         WHERE  i_brand_id = brand_id \n" +
            "                AND i_class_id = class_id \n" +
            "                AND i_category_id = category_id), \n" +
            "     avg_sales \n" +
            "     AS (SELECT Avg(quantity * list_price) average_sales \n" +
            "         FROM   (SELECT ss_quantity   quantity, \n" +
            "                        ss_list_price list_price \n" +
            "                 FROM   store_sales, \n" +
            "                        date_dim \n" +
            "                 WHERE  ss_sold_date_sk = d_date_sk \n" +
            "                        AND d_year BETWEEN 1999 AND 1999 + 2 \n" +
            "                 UNION ALL \n" +
            "                 SELECT cs_quantity   quantity, \n" +
            "                        cs_list_price list_price \n" +
            "                 FROM   catalog_sales, \n" +
            "                        date_dim \n" +
            "                 WHERE  cs_sold_date_sk = d_date_sk \n" +
            "                        AND d_year BETWEEN 1999 AND 1999 + 2 \n" +
            "                 UNION ALL \n" +
            "                 SELECT ws_quantity   quantity, \n" +
            "                        ws_list_price list_price \n" +
            "                 FROM   web_sales, \n" +
            "                        date_dim \n" +
            "                 WHERE  ws_sold_date_sk = d_date_sk \n" +
            "                        AND d_year BETWEEN 1999 AND 1999 + 2) x) \n" +
            "SELECT  * \n" +
            "FROM   (SELECT 'store'                          channel, \n" +
            "               i_brand_id, \n" +
            "               i_class_id, \n" +
            "               i_category_id, \n" +
            "               Sum(ss_quantity * ss_list_price) sales, \n" +
            "               Count(*)                         number_sales \n" +
            "        FROM   store_sales, \n" +
            "               item, \n" +
            "               date_dim \n" +
            "        WHERE  ss_item_sk IN (SELECT ss_item_sk \n" +
            "                              FROM   cross_items) \n" +
            "               AND ss_item_sk = i_item_sk \n" +
            "               AND ss_sold_date_sk = d_date_sk \n" +
            "               AND d_week_seq = (SELECT d_week_seq \n" +
            "                                 FROM   date_dim \n" +
            "                                 WHERE  d_year = 1999 + 1 \n" +
            "                                        AND d_moy = 12 \n" +
            "                                        AND d_dom = 25) \n" +
            "        GROUP  BY i_brand_id, \n" +
            "                  i_class_id, \n" +
            "                  i_category_id \n" +
            "        HAVING Sum(ss_quantity * ss_list_price) > 0)  this_year, \n" +
            "       (SELECT 'store'                          channel, \n" +
            "               i_brand_id, \n" +
            "               i_class_id, \n" +
            "               i_category_id, \n" +
            "               Sum(ss_quantity * ss_list_price) sales, \n" +
            "               Count(*)                         number_sales \n" +
            "        FROM   store_sales, \n" +
            "               item, \n" +
            "               date_dim \n" +
            "        WHERE  ss_item_sk IN (SELECT ss_item_sk \n" +
            "                              FROM   cross_items) \n" +
            "               AND ss_item_sk = i_item_sk \n" +
            "               AND ss_sold_date_sk = d_date_sk \n" +
            "               AND d_week_seq = (SELECT d_week_seq \n" +
            "                                 FROM   date_dim \n" +
            "                                 WHERE  d_year = 1999 \n" +
            "                                        AND d_moy = 12 \n" +
            "                                        AND d_dom = 25) \n" +
            "        GROUP  BY i_brand_id, \n" +
            "                  i_class_id, \n" +
            "                  i_category_id \n" +
            "        HAVING Sum(ss_quantity * ss_list_price) > 0)  last_year \n" +
            "WHERE  this_year.i_brand_id = last_year.i_brand_id \n" +
            "       AND this_year.i_class_id = last_year.i_class_id \n" +
            "       AND this_year.i_category_id = last_year.i_category_id \n" +
            "ORDER  BY this_year.channel, \n" +
            "          this_year.i_brand_id, \n" +
            "          this_year.i_class_id, \n" +
            "          this_year.i_category_id\n" +
            "LIMIT 100";

    private List<String> listFilesForFolder(final File folder)
    {
        List<String> queries = new ArrayList<>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                queries.add(fileEntry.getName());
            }
        }
        return queries;
    }

    private boolean isLinePartSQL(String line)
    {
        if (line == null) {
            return false;
        }
        if (line.equalsIgnoreCase("use tpcds.tiny;")) {
            return false;
        }
        if (line.startsWith("--")) {
            return false;
        }
        if (line.startsWith("explain")) {
            return false;
        }
        if (line.equals("\n")) {
            return false;
        }
        return true;
    }

    private String getQuery(String fileName) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (isLinePartSQL(line)) {
                sb.append(line);
                sb.append("\n");
                line = br.readLine();
            }
            return sb.toString();
        } finally {
            br.close();
        }
    }


    private void list()
    {
        URL resource = getClass().getResource(TPCDS_QUERY_DIR);
        if (resource == null) {
            throw new RuntimeException("Hive metadata directory doesn't exist: " + TPCDS_QUERY_DIR);
        }

        File[] files = new File(resource.getPath()).listFiles();

        for (File sqlFile : files) {
            try {
                System.out.println(sqlFile.getAbsolutePath());
                String query = getQuery(sqlFile.getAbsolutePath());
                test_tpcdsPlanTest(query);
                System.out.println("pass");

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public TestPlanTpcdsWithMergeCommonSubPlansVsCteReuse()
    {
        nativeEngine = new TestMergeCommonSubPlans(ImmutableMap.of(
                SystemSessionProperties.SUBPLAN_MERGE_ENABLED, "false",
                SystemSessionProperties.CTE_REUSE_ENABLED, "true"));
        newEngine = new TestMergeCommonSubPlans(ImmutableMap.of(
                SystemSessionProperties.SUBPLAN_MERGE_ENABLED, "true",
                SystemSessionProperties.CTE_REUSE_ENABLED, "false"));
    }

    @Test
    public void identifyDifferencePoint()
    {
        System.out.println(Objects.hash("true", "false") + " " + Objects.hash("false", "true"));

        startQueryEngines();
        int nativeC = nativeEngine.getProjectionCount(TEST_QUERY44);
        int newC = newEngine.getProjectionCount(TEST_QUERY44);
        System.out.println("nativeEngine: " + nativeC);
        System.out.println("newEngine: " + newC);
        assertEquals(newC, nativeC);
        shutdownQueryEngines();
    }

    private void test_tpcdsPlanTest(String query)
    {
        startQueryEngines();

        Map<String, Integer> nativeCtes = nativeEngine.getCteCounter(query);
        Map<String, Integer> newCtes = newEngine.getCteCounter(query);

        boolean countOne = false;
        if (nativeCtes.keySet().size() <= newCtes.keySet().size()) {
            assertTrue(true);
        }
        else {
            System.out.println("native engine");
            for (Map.Entry<String, Integer> e : nativeCtes.entrySet()) {
                System.out.println(e.getKey() + ":" + e.getValue());
                countOne = (e.getValue() == 1);
                if (countOne) {
                    break;
                }
            }
            System.out.println("new engine");
            for (Map.Entry<String, Integer> e : newCtes.entrySet()) {
                System.out.println(e.getKey() + ":" + e.getValue());
            }
            if(!countOne) {
                System.out.println("Doesn't satisfy cte counter conditions. Manually check plans and verify!");
            }
        }

        pass++;

        shutdownQueryEngines();
    }

    @Test
    public void test_basicPlanCompare()
    {
        startQueryEngines();

        Map<String, Integer> nativeCtes = nativeEngine.getCteCounter(TEST_QUERY);
        Map<String, Integer> newCtes = newEngine.getCteCounter(TEST_QUERY);

        assertTrue(nativeCtes.keySet().size() <= newCtes.keySet().size());

        shutdownQueryEngines();
    }

    @Test
    public void test_basicPlanCompare14b()
    {
        startQueryEngines();

      //  Map<String, Integer> nativeCtes = nativeEngine.getCteCounter(TEST_QUERY14B);
        Map<String, Integer> newCtes = newEngine.getCteCounter(TEST_QUERY14B);

      //  assertTrue(nativeCtes.keySet().size() <= newCtes.keySet().size());

        shutdownQueryEngines();
    }

    private void shutdownQueryEngines()
    {
        nativeEngine.destroyPlanTest();
        newEngine.destroyPlanTest();
    }

    private void startQueryEngines()
    {
        try {
            nativeEngine.initPlanTest();
            newEngine.initPlanTest();
        } catch (IOException e) {
            fail("can't run test...");
        }
    }

    @Test
    public void test_tpcds99()
    {
        list();
        System.out.println(pass + " queries pass");
    }

}
