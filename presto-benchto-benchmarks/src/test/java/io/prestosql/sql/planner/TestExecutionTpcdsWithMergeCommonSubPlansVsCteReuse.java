package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.prestosql.SystemSessionProperties;
import io.prestosql.plugin.tpcds.TpcdsPlugin;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static io.prestosql.sql.planner.TestPlanTpcdsWithMergeCommonSubPlansVsCteReuse.TEST_QUERY;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestExecutionTpcdsWithMergeCommonSubPlansVsCteReuse
{
    TestQueryExecutionWithCte nativeEngine;
    TestQueryExecutionWithCte newEngine;

    public class TestQueryExecutionWithCte
    {
        Map<String, String> extraProperties;
        DistributedQueryRunner queryRunner;

        public TestQueryExecutionWithCte(Map<String, String> extraProperties)
        {
            this.extraProperties = extraProperties;
        }

        public DistributedQueryRunner createQueryRunner()
                throws Exception
        {
            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                    .setExtraProperties(extraProperties)
                    .setNodeCount(2)
                    .build();

            try {
                queryRunner.installPlugin(new TpcdsPlugin());
                queryRunner.createCatalog("tpcds", "tpcds");
                return queryRunner;
            }
            catch (Exception e) {
                queryRunner.close();
                throw e;
            }
        }

        public void init()
        {
            try {
                this.queryRunner = createQueryRunner();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public MaterializedResult execute(String sql)
        {
            return queryRunner.execute(sql);
        }
    }

    public TestExecutionTpcdsWithMergeCommonSubPlansVsCteReuse()
    {
        nativeEngine = new TestQueryExecutionWithCte(ImmutableMap.of(
                SystemSessionProperties.SUBPLAN_MERGE_ENABLED, "false",
                SystemSessionProperties.CTE_REUSE_ENABLED, "true"));
        nativeEngine.init();
        newEngine = new TestQueryExecutionWithCte(ImmutableMap.of(
                SystemSessionProperties.SUBPLAN_MERGE_ENABLED, "true",
                SystemSessionProperties.CTE_REUSE_ENABLED, "false"));
        nativeEngine.init();
    }

}
