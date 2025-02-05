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
package io.prestosql.spi;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.DictionaryId;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.DictionaryId.randomDictionaryId;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Page
{
    public static final int INSTANCE_SIZE = ClassLayout.parseClass(Page.class).instanceSize() +
            (3 * ClassLayout.parseClass(AtomicLong.class).instanceSize());

    private static final Block[] EMPTY_BLOCKS = new Block[0];

    static Page wrapBlocksWithoutCopy(int positionCount, Block[] blocks)
    {
        return new Page(false, positionCount, blocks);
    }

    private final Block[] blocks;
    private final int positionCount;
    private final AtomicLong sizeInBytes = new AtomicLong(-1);
    private final AtomicLong retainedSizeInBytes = new AtomicLong(-1);
    private final AtomicLong logicalSizeInBytes = new AtomicLong(-1);

    private Properties pageMetadata = new Properties();

    public Page(Block... blocks)
    {
        this(determinePositionCount(blocks), blocks);
    }

    public Page(Properties pageMetadata, Block... blocks)
    {
        this(determinePositionCount(blocks), pageMetadata, blocks);
    }

    public Page(int positionCount, Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        this.blocks = Arrays.copyOf(blocks, blocks.length);
        this.positionCount = positionCount;
    }

    private Page(boolean blocksCopyRequired, int positionCount, Block[] blocks)
    {
        requireNonNull(blocks, "blocks is null");
        this.positionCount = positionCount;
        if (blocks.length == 0) {
            this.blocks = EMPTY_BLOCKS;
            this.sizeInBytes.set(0L);
            this.logicalSizeInBytes.set(0L);
            // Empty blocks are not considered "retained" by any particular page
            this.retainedSizeInBytes.set(INSTANCE_SIZE);
        }
        else {
            this.blocks = blocksCopyRequired ? blocks.clone() : blocks;
        }
    }

    public Page(int positionCount, Properties pageMetadata, Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        this.blocks = Arrays.copyOf(blocks, blocks.length);
        this.positionCount = positionCount;
        this.pageMetadata = pageMetadata;
    }

    public int getChannelCount()
    {
        return blocks.length;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public long getSizeInBytes()
    {
        long size = this.sizeInBytes.get();
        if (size < 0) {
            size = 0;
            for (Block block : blocks) {
                size += block.getSizeInBytes();
            }
            this.sizeInBytes.set(size);
        }
        return size;
    }

    public long getLogicalSizeInBytes()
    {
        long size = logicalSizeInBytes.get();
        if (size < 0) {
            size = 0;
            for (Block block : blocks) {
                size += block.getLogicalSizeInBytes();
            }
            logicalSizeInBytes.set(size);
        }
        return size;
    }

    public long getRetainedSizeInBytes()
    {
        if (retainedSizeInBytes.get() < 0) {
            updateRetainedSize();
        }
        return retainedSizeInBytes.get();
    }

    public Block getBlock(int channel)
    {
        return blocks[channel];
    }

    /**
     * Gets the values at the specified position as a single element page.  The method creates independent
     * copy of the data.
     */
    public Page getSingleValuePage(int position)
    {
        Block[] singleValueBlocks = new Block[this.blocks.length];
        for (int i = 0; i < this.blocks.length; i++) {
            singleValueBlocks[i] = this.blocks[i].getSingleValueBlock(position);
        }
        return new Page(1, singleValueBlocks);
    }

    public Page getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException(format("Invalid position %s and length %s in page with %s positions", positionOffset, length, positionCount));
        }

        int channelCount = getChannelCount();
        Block[] slicedBlocks = new Block[channelCount];
        for (int i = 0; i < channelCount; i++) {
            slicedBlocks[i] = blocks[i].getRegion(positionOffset, length);
        }
        return new Page(length, slicedBlocks);
    }

    public Page appendColumn(Block block)
    {
        requireNonNull(block, "block is null");
        if (positionCount != block.getPositionCount()) {
            throw new IllegalArgumentException("Block does not have same position count");
        }

        Block[] newBlocks = Arrays.copyOf(blocks, blocks.length + 1);
        newBlocks[blocks.length] = block;
        return new Page(newBlocks);
    }

    public void compact()
    {
        if (getRetainedSizeInBytes() <= getSizeInBytes()) {
            return;
        }

        for (int i = 0; i < blocks.length; i++) {
            Block block = blocks[i];
            if (block instanceof DictionaryBlock) {
                continue;
            }
            // Compact the block
            blocks[i] = block.copyRegion(0, block.getPositionCount());
        }

        Map<DictionaryId, DictionaryBlockIndexes> dictionaryBlocks = getRelatedDictionaryBlocks();
        for (DictionaryBlockIndexes blockIndexes : dictionaryBlocks.values()) {
            List<DictionaryBlock> compactBlocks = compactRelatedBlocks(blockIndexes.getBlocks());
            List<Integer> indexes = blockIndexes.getIndexes();
            for (int i = 0; i < compactBlocks.size(); i++) {
                blocks[indexes.get(i)] = compactBlocks.get(i);
            }
        }

        updateRetainedSize();
    }

    private Map<DictionaryId, DictionaryBlockIndexes> getRelatedDictionaryBlocks()
    {
        Map<DictionaryId, DictionaryBlockIndexes> relatedDictionaryBlocks = new HashMap<>();

        for (int i = 0; i < blocks.length; i++) {
            Block block = blocks[i];
            if (block instanceof DictionaryBlock) {
                DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
                relatedDictionaryBlocks.computeIfAbsent(dictionaryBlock.getDictionarySourceId(), id -> new DictionaryBlockIndexes())
                        .addBlock(dictionaryBlock, i);
            }
        }
        return relatedDictionaryBlocks;
    }

    private static List<DictionaryBlock> compactRelatedBlocks(List<DictionaryBlock> blocks)
    {
        DictionaryBlock firstDictionaryBlock = blocks.get(0);
        Block dictionary = firstDictionaryBlock.getDictionary();

        int count = firstDictionaryBlock.getPositionCount();
        int dictionarySize = dictionary.getPositionCount();

        // determine which dictionary entries are referenced and build a reindex for them
        int[] dictionaryPositionsToCopy = new int[min(dictionarySize, count)];
        int[] remapIndex = new int[dictionarySize];
        Arrays.fill(remapIndex, -1);

        int numberOfIndexes = 0;
        for (int i = 0; i < count; i++) {
            int position = firstDictionaryBlock.getId(i);
            if (remapIndex[position] == -1) {
                dictionaryPositionsToCopy[numberOfIndexes] = position;
                remapIndex[position] = numberOfIndexes;
                numberOfIndexes++;
            }
        }

        // entire dictionary is referenced
        if (numberOfIndexes == dictionarySize) {
            return blocks;
        }

        // compact the dictionaries
        int[] newIds = getNewIds(count, firstDictionaryBlock, remapIndex);
        List<DictionaryBlock> outputDictionaryBlocks = new ArrayList<>(blocks.size());
        DictionaryId newDictionaryId = randomDictionaryId();
        for (DictionaryBlock dictionaryBlock : blocks) {
            if (!firstDictionaryBlock.getDictionarySourceId().equals(dictionaryBlock.getDictionarySourceId())) {
                throw new IllegalArgumentException("dictionarySourceIds must be the same");
            }

            try {
                Block compactDictionary = dictionaryBlock.getDictionary().copyPositions(dictionaryPositionsToCopy, 0, numberOfIndexes);
                outputDictionaryBlocks.add(new DictionaryBlock(count, compactDictionary, newIds, true, newDictionaryId));
            }
            catch (UnsupportedOperationException e) {
                // ignore if copy positions is not supported for the dictionary
                outputDictionaryBlocks.add(dictionaryBlock);
            }
        }
        return outputDictionaryBlocks;
    }

    private static int[] getNewIds(int positionCount, DictionaryBlock dictionaryBlock, int[] remapIndex)
    {
        int[] newIds = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            int newId = remapIndex[dictionaryBlock.getId(i)];
            if (newId == -1) {
                throw new IllegalStateException("reference to a non-existent key");
            }
            newIds[i] = newId;
        }
        return newIds;
    }

    /**
     * Returns a page that assures all data is in memory.
     * May return the same page if all page data is already in memory.
     * <p>
     * This allows streaming data sources to skip sections that are not
     * accessed in a query.
     */
    public Page getLoadedPage()
    {
        boolean allLoaded = true;
        Block[] loadedBlocks = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            loadedBlocks[i] = blocks[i].getLoadedBlock();
            if (loadedBlocks[i] != blocks[i]) {
                allLoaded = false;
            }
        }

        if (allLoaded) {
            return this;
        }

        return new Page(this.pageMetadata, loadedBlocks);
    }

    public Page getLoadedPage(int... columns)
    {
        requireNonNull(columns, "columns is null");

        Block[] blockArr = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            blockArr[i] = this.blocks[columns[i]].getLoadedBlock();
        }
        return wrapBlocksWithoutCopy(positionCount, blockArr);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Page{");
        builder.append("positions=").append(positionCount);
        builder.append(", channels=").append(getChannelCount());
        builder.append('}');
        builder.append("@").append(Integer.toHexString(System.identityHashCode(this)));
        return builder.toString();
    }

    private static int determinePositionCount(Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }

        return blocks[0].getPositionCount();
    }

    public Page getPositions(int[] retainedPositions, int offset, int length)
    {
        requireNonNull(retainedPositions, "retainedPositions is null");

        Block[] blockArrays = new Block[this.blocks.length];
        Arrays.setAll(blockArrays, i -> this.blocks[i].getPositions(retainedPositions, offset, length));
        return new Page(length, blockArrays);
    }

    public Page getColumns(int... columns)
    {
        requireNonNull(columns, "columns is null");

        Block[] block = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            block[i] = this.blocks[columns[i]];
        }
        return wrapBlocksWithoutCopy(positionCount, block);
    }

    public Page prependColumn(Block column)
    {
        if (column.getPositionCount() != positionCount) {
            throw new IllegalArgumentException(format("Column does not have same position count (%s) as page (%s)", column.getPositionCount(), positionCount));
        }

        Block[] result = new Block[blocks.length + 1];
        result[0] = column;
        System.arraycopy(blocks, 0, result, 1, blocks.length);

        return new Page(positionCount, result);
    }

    private void updateRetainedSize()
    {
        long instanceSizeInBytes = INSTANCE_SIZE + sizeOf(blocks);
        for (Block block : blocks) {
            instanceSizeInBytes += block.getRetainedSizeInBytes();
        }
        this.retainedSizeInBytes.set(instanceSizeInBytes);
    }

    private static class DictionaryBlockIndexes
    {
        private final List<DictionaryBlock> blocks = new ArrayList<>();
        private final List<Integer> indexes = new ArrayList<>();

        public void addBlock(DictionaryBlock block, int index)
        {
            blocks.add(block);
            indexes.add(index);
        }

        public List<DictionaryBlock> getBlocks()
        {
            return blocks;
        }

        public List<Integer> getIndexes()
        {
            return indexes;
        }
    }

    public Properties getPageMetadata()
    {
        return pageMetadata;
    }

    public void setPageMetadata(String key, String value)
    {
        pageMetadata.setProperty(key, value);
    }

    public Block[] getBlocks()
    {
        return blocks;
    }
}
