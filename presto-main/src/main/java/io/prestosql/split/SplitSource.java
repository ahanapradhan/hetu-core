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
package io.prestosql.split;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.execution.Lifespan;
import io.prestosql.metadata.Split;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorPartitionHandle;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface SplitSource
        extends Closeable
{
    CatalogName getCatalogName();

    ListenableFuture<SplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize);

    @Override
    void close();

    boolean isFinished();

    default Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return Optional.empty();
    }

    class SplitBatch
    {
        private final List<Split> splits;
        private final boolean lastBatch;

        public SplitBatch(List<Split> splits, boolean lastBatch)
        {
            this.splits = requireNonNull(splits, "splits is null");
            this.lastBatch = lastBatch;
        }

        public List<Split> getSplits()
        {
            return splits;
        }

        /**
         * Returns <tt>true</tt> if all splits for the requested driver group have been returned.
         * In other hands, splits returned from this and all previous invocations of {@link #getNextBatch}
         * form the complete set of splits in the requested driver group.
         */
        public boolean isLastBatch()
        {
            return lastBatch;
        }
    }

    default List<Split> groupSmallSplits(List<Split> pendingSplits, Lifespan lifespan, int maxGroupSize)
    {
        return pendingSplits;
    }
}
