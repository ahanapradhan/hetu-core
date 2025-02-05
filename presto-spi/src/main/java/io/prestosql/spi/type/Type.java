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
package io.prestosql.spi.type;

import com.fasterxml.jackson.annotation.JsonValue;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Stream;

import static io.prestosql.spi.type.TypeOperatorDeclaration.NO_TYPE_OPERATOR_DECLARATION;
import static java.util.Objects.requireNonNull;

public interface Type
{
    default TypeId getTypeId()
    {
        return TypeId.of(getTypeSignature().toString());
    }

    Set<ColumnHandle> set = new CopyOnWriteArraySet<>();

    default void setUpdateColumnHandle(ColumnHandle columnHandle)
    {
        set.add(columnHandle);
    }

    default Set<ColumnHandle> getUpdateColumnHandle()
    {
        return set;
    }

    default void clear()
    {
        set.clear();
    }

    /**
     * Gets the name of this type which must be case insensitive globally unique.
     * The name of a user defined type must be a legal identifier in Presto.
     */
    @JsonValue
    TypeSignature getTypeSignature();

    /**
     * Returns the name of this type that should be displayed to end-users.
     */
    String getDisplayName();

    /**
     * True if the type supports equalTo and hash.
     */
    boolean isComparable();

    /**
     * True if the type supports compareTo.
     */
    boolean isOrderable();

    /**
     * Gets the Java class type used to represent this value on the stack during
     * expression execution.
     * <p>
     * Currently, this must be boolean, long, double, Slice or Block.
     */
    Class<?> getJavaType();

    /**
     * For parameterized types returns the list of parameters.
     */
    List<Type> getTypeParameters();

    /**
     * Creates the preferred block builder for this type. This is the builder used to
     * store values after an expression projection within the query.
     */
    BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry);

    /**
     * Creates the preferred block builder for this type. This is the builder used to
     * store values after an expression projection within the query.
     */
    BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries);

    /**
     * Returns the value in the block with the proper type
     * @param <T>
     * @return
     */
    default <T> T get(Block<T> block, int position)
    {
        //FIXME: KEN: we shouldn't need to pass in the position most of the time
        return block.get(position);
    }

    /**
     * Gets an object representation of the type value in the {@code block}
     * {@code position}. This is the value returned to the user via the
     * REST endpoint and therefore must be JSON serializable.
     */
    <T> Object getObjectValue(ConnectorSession session, Block<T> block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as a boolean.
     */
    boolean getBoolean(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as a long.
     */
    long getLong(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as a double.
     */
    double getDouble(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as a Slice.
     */
    Slice getSlice(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as an Object.
     */
    <T> Object getObject(Block<T> block, int position);

    /**
     * Writes the boolean value into the {@code BlockBuilder}.
     */
    void writeBoolean(BlockBuilder blockBuilder, boolean value);

    /**
     * Writes the long value into the {@code BlockBuilder}.
     */
    void writeLong(BlockBuilder blockBuilder, long value);

    /**
     * Writes the double value into the {@code BlockBuilder}.
     */
    void writeDouble(BlockBuilder blockBuilder, double value);

    /**
     * Writes the Slice value into the {@code BlockBuilder}.
     */
    void writeSlice(BlockBuilder blockBuilder, Slice value);

    /**
     * Writes the Slice value into the {@code BlockBuilder}.
     */
    void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length);

    /**
     * Writes the Object value into the {@code BlockBuilder}.
     */
    void writeObject(BlockBuilder blockBuilder, Object value);

    /**
     * Append the value at {@code position} in {@code block} to {@code blockBuilder}.
     */
    void appendTo(Block block, int position, BlockBuilder blockBuilder);

    /**
     * Are the values in the specified blocks at the specified positions equal?
     * <p>
     *. This method assumes input is not null.
     */
    <T> boolean equalTo(Block<T> leftBlock, int leftPosition, Block<T> rightBlock, int rightPosition);

    /**
     * Calculates the hash code of the value at the specified position in the
     * specified block.
     */
    <T> long hash(Block<T> block, int position);

    /**
     * Compare the values in the specified block at the specified positions.
     *
     * @return 0 if the values are equal, negative if left is less than right, and positive, otherwise.
     */
    <T> int compareTo(Block<T> leftBlock, int leftPosition, Block<T> rightBlock, int rightPosition);

    /**
     * Return the range of possible values for this type, if available.
     *
     * The type of the values must match {@link #getJavaType}
     */
    default Optional<Range> getRange()
    {
        return Optional.empty();
    }

    /**
     * Returns a stream of discrete values inside the specified range (if supported by this type).
     */
    default Optional<Stream<?>> getDiscreteValues(Range range)
    {
        return Optional.empty();
    }

    final class Range
    {
        private final Object min;
        private final Object max;

        public Range(Object min, Object max)
        {
            this.min = requireNonNull(min, "min is null");
            this.max = requireNonNull(max, "max is null");
        }

        public Object getMin()
        {
            return min;
        }

        public Object getMax()
        {
            return max;
        }
    }

    default <T> T read(RawInput<T> input)
    {
        throw new NotImplementedException();
    }

    default <T> void write(BlockBuilder<T> blockBuilder, T value)
    {
        throw new NotImplementedException();
    }

    default TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return NO_TYPE_OPERATOR_DECLARATION;
    }
}
