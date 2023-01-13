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

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTpcdsWithMergeCommonSubPlansVsCteReuse
{
    int pass = 0;
    public static final String TPCDS_QUERY_DIR = "/for_testcase";
    TestMergeCommonSubPlans nativeEngine;
    TestMergeCommonSubPlans newEngine;

    private static final String TEST_QUERY = "WITH best_ss_customer AS " +
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

    public TestTpcdsWithMergeCommonSubPlansVsCteReuse()
    {
        nativeEngine = new TestMergeCommonSubPlans(ImmutableMap.of(
                SystemSessionProperties.SUBPLAN_MERGE_ENABLED, "false",
                SystemSessionProperties.CTE_REUSE_ENABLED, "true"));
        newEngine = new TestMergeCommonSubPlans(ImmutableMap.of(
                SystemSessionProperties.SUBPLAN_MERGE_ENABLED, "true",
                SystemSessionProperties.CTE_REUSE_ENABLED, "false"));
    }

    private void test_tpcdsPlanTest(String query)
    {
        try {
            nativeEngine.initPlanTest();
            newEngine.initPlanTest();
        } catch (IOException e) {
            fail("can't run test...");
        }

        Map<String, Integer> nativeCtes = nativeEngine.getCteCounter(query);
        Map<String, Integer> newCtes = newEngine.getCteCounter(query);

        boolean countOne = false;
        if (nativeCtes.keySet().size() > newCtes.keySet().size()) {
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
            assertTrue(countOne);
        }
        else {
            assertTrue(true);
        }
        pass++;

        nativeEngine.destroyPlanTest();
        newEngine.destroyPlanTest();
    }

    @Test
    public void test_basicPlanCompare()
    {
        try {
            nativeEngine.initPlanTest();
            newEngine.initPlanTest();
        } catch (IOException e) {
            fail("can't run test...");
        }

        Map<String, Integer> nativeCtes = nativeEngine.getCteCounter(TEST_QUERY);
        Map<String, Integer> newCtes = newEngine.getCteCounter(TEST_QUERY);

        assertTrue(nativeCtes.keySet().size() <= newCtes.keySet().size());

        nativeEngine.destroyPlanTest();
        newEngine.destroyPlanTest();
    }

    @Test
    public void test_tpcds99()
    {
        list();
        System.out.println(pass + " queries pass");
    }
}
