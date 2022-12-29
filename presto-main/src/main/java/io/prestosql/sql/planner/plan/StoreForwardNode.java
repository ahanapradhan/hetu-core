package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;

import javax.annotation.concurrent.Immutable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Immutable
public class StoreForwardNode
        extends InternalPlanNode
{

    private final String fileName;
    private final PlanNode source;
    private final List<Symbol> outputSymbols; // column name = symbol

    public Map<String, Symbol> symbolNamesToAbsoluteNames = new HashMap<String, Symbol>();

    @JsonCreator
    public StoreForwardNode(@JsonProperty("id") PlanNodeId id,
                            @JsonProperty("source") PlanNode source,
                            @JsonProperty("fileName") String fileName,
                            @JsonProperty("outputSymbols") List<Symbol> outputSymbols)
    {
        super(id);

        this.NODE_TYPE_NAME = "StoreForwardNode";
        requireNonNull(source, "source is null");

        this.source = source;
        this.fileName = fileName;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public String getFileName ()
    {
        return fileName;
    }

    @JsonProperty
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitStoreForward(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new StoreForwardNode(getId(), Iterables.getOnlyElement(newChildren), fileName, outputSymbols);
    }
}
