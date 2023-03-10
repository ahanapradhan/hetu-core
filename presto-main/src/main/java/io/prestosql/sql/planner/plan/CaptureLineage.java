package io.prestosql.sql.planner.plan;


import io.airlift.slice.Slice;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.*;
import io.prestosql.spi.relation.*;
import io.prestosql.spi.type.*;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Row;
import org.weakref.jmx.internal.guava.collect.ImmutableList;

import javax.validation.constraints.Past;
import java.util.*;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.BigintType.BIGINT;

//traverse the plan to find flattened subexpressions (we don't care about operator order)
//we focus on Select-Project-Join subexpressions
public class CaptureLineage
        extends InternalPlanVisitor<CaptureLineage.Lineage, Void> {
    StatsAndCosts statsAndCosts;
    TypeProvider typeProvider;

    Map<PlanNodeId, Lineage> lineageMapping;

    String joinHintStr = null;
    List<Lineage> selectedViews = null;

    Lineage lastValid;

    public CaptureLineage(StatsAndCosts statsAndCosts, TypeProvider typeProvider) {
        this.statsAndCosts = statsAndCosts;
        this.typeProvider = typeProvider;
        lineageMapping = new HashMap<>();
        lastValid = null;
    }

    public Lineage getLastValid() {
        return lastValid;
    }

    public String getJoinHintString() {
        return joinHintStr;
    }

    public void setJoinHintString(String joinHintStr) {
        this.joinHintStr = new String(joinHintStr);
    }

    public List<Lineage> getSelectedViews() {
        return selectedViews;
    }

    public void setSelectedViews(List<Lineage> views) {
        this.selectedViews = views;
    }

    public Map<PlanNodeId, Lineage> getLineageMapping() {
        return lineageMapping;
    }

    public Lineage visitRemoteSource(RemoteSourceNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitOutput(OutputNode node, Void context) {
        Lineage lineage = visitPlan(node.getSource(), context);
        lineage = new Lineage(lineage);

        //lineageMapping.put(node.getId(), lineage);

        if (lineage.isValid()) {
            lastValid = lineage;
        }

        return lineage;
    }

    public Lineage visitOffset(OffsetNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitCreateIndex(CreateIndexNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitDistinctLimit(DistinctLimitNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitSample(SampleNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitExplainAnalyze(ExplainAnalyzeNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitIndexSource(IndexSourceNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitSemiJoin(SemiJoinNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitSpatialJoin(SpatialJoinNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitIndexJoin(IndexJoinNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitSort(SortNode node, Void context) {
        Lineage lineage = visitPlan(node.getSource(), context);
        lineage = new Lineage(lineage);
        return lineage;
        // throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitTableWriter(TableWriterNode node, Void context) {
        Lineage lineage = visitPlan(node.getSource(), context);
        lineage = new Lineage(lineage);
        lineage.finalize(node.getTarget());
        lineageMapping.put(node.getId(), lineage);
        if (lineage.isValid()) {
            lastValid = lineage;
        }
        return lineage;
    }

    public Lineage visitDelete(DeleteNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitUpdate(UpdateNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitVacuumTable(VacuumTableNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitTableDelete(TableDeleteNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitTableFinish(TableFinishNode node, Void context) {
        Lineage lineage = visitPlan(node.getSource(), context);

        if (!lineage.isFinalized()) {
            throw new UnsupportedOperationException("not implemented");
        }

        lineage = new Lineage(lineage);

        lineageMapping.put(node.getId(), lineage);

        if (lineage.isValid()) {
            lastValid = lineage;
        }

        return lineage;
    }

    public Lineage visitCubeFinish(CubeFinishNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitStatisticsWriterNode(StatisticsWriterNode node, Void context) {
        Lineage lineage = visitPlan(node.getSource(), context);

//        if (!lineage.isFinalized()) {
//            throw new UnsupportedOperationException("not implemented");
//        }

        lineage = new Lineage(lineage);

        lineageMapping.put(node.getId(), lineage);

        if (lineage.isValid()) {
            lastValid = lineage;
        }

        return lineage;
    }

    public Lineage visitUnnest(UnnestNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitRowNumber(RowNumberNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitTopNRankingNumber(TopNRankingNumberNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitExchange(ExchangeNode node, Void context) {
        if (node.getSources().size() != 1) {
            throw new UnsupportedOperationException("not implemented");
        }

        Lineage lineage = visitPlan(node.getSources().get(0), context);

        if (!node.getPartitioningScheme().getPartitioning().getColumns().isEmpty()) {
            lineage = Lineage.createPartitioned(lineage, statsAndCosts.getStats().get(node.getId()), node.getPartitioningScheme().getPartitioning().getColumns().stream().collect(Collectors.toList()), node.getOutputSymbols().stream().map(symbol -> symbol.getName()).collect(Collectors.toList()));
        } else {
            lineage = new Lineage(lineage);
        }

        lineageMapping.put(node.getId(), lineage);

        if (lineage.isValid()) {
            lastValid = lineage;
        }

        return lineage;
    }

    public Lineage visitEnforceSingleRow(EnforceSingleRowNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitApply(ApplyNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitAssignUniqueId(AssignUniqueId node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitLateralJoin(LateralJoinNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }


    public Lineage visitPlan(PlanNode node, Void context) {
        return node.accept(this, context);
    }

    public Lineage visitAggregation(AggregationNode node, Void context) {
        visitPlan(node.getSource(), context);
        lineageMapping.put(node.getId(), new Lineage(false));
        return new Lineage(false);
    }

    public Lineage visitStoreForward(StoreForwardNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitExcept(ExceptNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitFilter(FilterNode node, Void context) {
        Lineage lineage = visitPlan(node.getSource(), context);

        if (lineage.isFinalized()) {
            throw new UnsupportedOperationException("not implemented");
        }

        if (!lineage.isValid()) {
            return new Lineage(false);
        }

        Lineage ret = Lineage.createFilter(lineage, statsAndCosts.getStats().get(node.getId()), node.getPredicate(), node.getOutputSymbols().stream().map(symbol -> symbol.getName()).collect(Collectors.toList()));

        lineageMapping.put(node.getId(), ret);

        if (ret.isValid()) {
            lastValid = ret;
        }

        return ret;
    }

    public Lineage visitIntersect(IntersectNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitJoin(JoinNode node, Void context) {
        Lineage lineage1 = visitPlan(node.getLeft(), context);
        Lineage lineage2 = visitPlan(node.getRight(), context);

        if (lineage1.isFinalized() || lineage2.isFinalized()) {
            throw new UnsupportedOperationException("not implemented");
        }

        if (!lineage1.isValid() || !lineage2.isValid()) {
            return new Lineage(false);
        }

        Lineage ret = Lineage.createJoin(lineage1, lineage2, statsAndCosts.getStats().get(node.getId()), node.getCriteria(), node.getFilter(), node.getOutputSymbols().stream().map(symbol -> symbol.getName()).collect(Collectors.toList()));

        lineageMapping.put(node.getId(), ret);

        if (ret.isValid()) {
            lastValid = ret;
        }

        return ret;
    }

    public Lineage visitLimit(LimitNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitMarkDistinct(MarkDistinctNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitProject(ProjectNode node, Void context) {
        Lineage lineageOld = visitPlan(node.getSource(), context);

        if (lineageOld.isFinalized()) {
            throw new UnsupportedOperationException("not implemented");
        }

        if (!lineageOld.isValid()) {
            return new Lineage(false);
        }

        Lineage lineage = Lineage.createProject(lineageOld, node.getOutputSymbols().stream().map(symbol -> symbol.getName()).collect(Collectors.toList()));

        lineageMapping.put(node.getId(), lineage);

        if (lineage.isValid()) {
            lastValid = lineage;
        }

        return lineage;
    }

    public Lineage visitTableScan(TableScanNode node, Void context) {
        Map<String, Type> attributeToType = new HashMap<>();
        for (Symbol symbol : node.getOutputSymbols()) {
            //System.out.println(symbol.getName() + " " + typeProvider.get(symbol))
            attributeToType.put(symbol.getName(), typeProvider.get(symbol));
        }

        Lineage lineage = Lineage.createBaseTable(node.getTable().getFullyQualifiedName(), statsAndCosts.getStats().get(node.getId()), attributeToType, node.getOutputSymbols().stream().map(symbol -> symbol.getName()).collect(Collectors.toList()), Optional.empty());
        lineageMapping.put(node.getId(), lineage);
        if (lineage.isValid()) {
            lastValid = lineage;
        }
        return lineage;
    }

    public Lineage visitTopN(TopNNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitUnion(UnionNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitValues(ValuesNode node, Void context) {
        return new Lineage(false);
    }

    public Lineage visitGroupReference(GroupReference node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitWindow(WindowNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitGroupId(GroupIdNode node, Void context) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Lineage visitCTEScan(CTEScanNode node, Void context) {
        throw new IllegalArgumentException("this should not happen");
    }

    //condition expression expresses a conjunctive query for a subexpression
    //for each attribute (key of groupedFilters, we have RowExpressions that signify filters
    //we keep ranges as list(0)=min filter and list(1)=max filter
    public static class ConditionExpression {
        Map<String, List<RowExpression>> groupedFilters;

        public ConditionExpression(Map<String, List<RowExpression>> groupedFilters) {
            this.groupedFilters = groupedFilters;
        }

        public String toString() {
            String ret = "";
            for (Map.Entry<String, List<RowExpression>> entry : groupedFilters.entrySet()) {
                String conditions = "";

                ret = ret + entry.getKey() + " ";

                for (RowExpression expression : entry.getValue()) {
                    ret = ret + expression.toString() + " ";
                }

                ret = ret + "\n";
            }

            return ret;
        }

        //check if this subsumes other
        public boolean subsumes(ConditionExpression other) {
            for (Map.Entry<String, List<RowExpression>> entry : groupedFilters.entrySet()) {
                if (!other.groupedFilters.containsKey(entry.getKey()))
                    return false;
            }

            for (Map.Entry<String, List<RowExpression>> entry : other.groupedFilters.entrySet()) {
                if (groupedFilters.containsKey(entry.getKey())) {
                    CallExpression callExpressionThisMin = (CallExpression) groupedFilters.get(entry.getKey()).get(0);
                    if (!callExpressionThisMin.getDisplayName().equals("$operator$equals") &&
                            !callExpressionThisMin.getDisplayName().equals("$operator$greater_than") &&
                            !callExpressionThisMin.getDisplayName().equals("$operator$greater_than_or_equal"))
                        callExpressionThisMin = null;

                    CallExpression callExpressionThisMax = (CallExpression) groupedFilters.get(entry.getKey()).get(groupedFilters.get(entry.getKey()).size() - 1);
                    if (!callExpressionThisMax.getDisplayName().equals("$operator$equals") &&
                            !callExpressionThisMax.getDisplayName().equals("$operator$less_than") &&
                            !callExpressionThisMax.getDisplayName().equals("$operator$less_than_or_equal"))
                        callExpressionThisMax = null;

                    CallExpression callExpressionOtherMin = (CallExpression) other.groupedFilters.get(entry.getKey()).get(0);
                    if (!callExpressionOtherMin.getDisplayName().equals("$operator$equals") &&
                            !callExpressionOtherMin.getDisplayName().equals("$operator$greater_than") &&
                            !callExpressionOtherMin.getDisplayName().equals("$operator$greater_than_or_equal"))
                        callExpressionOtherMin = null;

                    CallExpression callExpressionOtherMax = (CallExpression) other.groupedFilters.get(entry.getKey()).get(other.groupedFilters.get(entry.getKey()).size() - 1);
                    if (!callExpressionOtherMax.getDisplayName().equals("$operator$equals") &&
                            !callExpressionOtherMax.getDisplayName().equals("$operator$less_than") &&
                            !callExpressionOtherMax.getDisplayName().equals("$operator$less_than_or_equal"))
                        callExpressionOtherMax = null;

                    if (callExpressionThisMin != null &&
                            (callExpressionOtherMin == null
                                    || inferredTypeComparison(
                                    ((ConstantExpression) callExpressionThisMin.getArguments().get(1)).getValue(),
                                    ((ConstantExpression) callExpressionOtherMin.getArguments().get(1)).getValue(),
                                    ((ConstantExpression) callExpressionThisMin.getArguments().get(1)).getType()) > 0))
                        return false;

                    if (callExpressionThisMax != null &&
                            (callExpressionOtherMax == null
                                    || inferredTypeComparison(
                                    ((ConstantExpression) callExpressionThisMax.getArguments().get(1)).getValue(),
                                    ((ConstantExpression) callExpressionOtherMax.getArguments().get(1)).getValue(),
                                    ((ConstantExpression) callExpressionThisMax.getArguments().get(1)).getType()) < 0))
                        return false;
                }
            }

            return true;
        }

        public static int inferredTypeComparison(Object obj1, Object obj2, Type type) {
            if (type instanceof BigintType) {
                return ((Long) obj1).compareTo((Long) obj2);
            } else if (type instanceof IntegerType) {
                return ((Long) obj1).compareTo((Long) obj2);
            } else if (type instanceof BooleanType) {
                return ((Boolean) obj1).compareTo((Boolean) obj2);
            } else if (type instanceof VarcharType) {
                if (obj1 instanceof io.airlift.slice.Slice && obj2 instanceof io.airlift.slice.Slice) {
                    String obj1string = ((Slice) obj1).toStringUtf8();
                    String obj2string = ((Slice) obj2).toStringUtf8();
                    return obj1string.compareTo(obj2string);
                }
                return ((String) obj1).compareTo((String) obj2);
            } else if (type instanceof DecimalType) {
                return ((Long) obj1).compareTo((Long) obj2);
            }

            throw new UnsupportedOperationException("not handled " + type.toString());
        }

        //simplify expression to get a simple between predicate
        public static List<RowExpression> optimizeExpressionOnAttribute(List<RowExpression> groupedFilters) {
            if (groupedFilters.isEmpty()) return groupedFilters;

            Type attrType = null;

            int minIdx = -1;
            int maxIdx = -1;
            int idx = 0;

            Object minVal = null;
            Object maxVal = null;

            boolean minInclusive = false;
            boolean maxInclusive = false;

            for (RowExpression expression : groupedFilters) {
                if (expression instanceof CallExpression) {
                    CallExpression callExpression = (CallExpression) expression;
                    Object obj = ((ConstantExpression) callExpression.getArguments().get(1)).getValue();
                    Type type = ((ConstantExpression) callExpression.getArguments().get(1)).getType();

                    attrType = type;

                    System.out.println(callExpression.getDisplayName());
                    String operator = callExpression.getDisplayName();

                    if (operator.equals("$operator$less_than_or_equal") || operator.equals("LESS_THAN_OR_EQUAL")) {
                        if (maxVal == null || inferredTypeComparison(maxVal, obj, type) > 0) {
                            maxVal = obj;
                            maxInclusive = true;
                            maxIdx = idx;
                        }
                    } else if (operator.equals("$operator$less_than") || operator.equals("LESS_THAN")) {
                        if (maxVal == null || inferredTypeComparison(maxVal, obj, type) > 0) {
                            maxVal = obj;
                            maxInclusive = false;
                            maxIdx = idx;
                        }
                    } else if (operator.equals("$operator$greater_than_or_equal") || operator.equals("GREATER_THAN_OR_EQUAL")) {
                        if (minVal == null || inferredTypeComparison(minVal, obj, type) < 0) {
                            minVal = obj;
                            minInclusive = false;
                            minIdx = idx;
                        }
                    } else if (operator.equals("$operator$greater_than") || operator.equals("GREATER_THAN")) {
                        if (minVal == null || inferredTypeComparison(minVal, obj, type) < 0) {
                            minVal = obj;
                            minInclusive = false;
                            minIdx = idx;
                        }
                    } else if (operator.equals("$operator$equal") || operator.equals("EQUAL")) {
                        if (minVal == null) {
                            minVal = obj;
                            minInclusive = true;
                            minIdx = idx;
                        } else {
                            if (inferredTypeComparison(minVal, obj, type) != 0) return null;
                        }

                        if (maxVal == null) {
                            maxVal = obj;
                            maxInclusive = true;
                            maxIdx = idx;
                        } else {
                            if (inferredTypeComparison(maxVal, obj, type) != 0) return null;
                        }
                    } else {
                        throw new UnsupportedOperationException("not handled");
                    }
                } else {
                    throw new UnsupportedOperationException("not handled");
                }

                idx++;
            }

            if (minVal != null && maxVal != null && inferredTypeComparison(minVal, maxVal, attrType) > 0) {
                return null;
            }

            List<RowExpression> expressions = new ArrayList<>();

            if (minIdx == maxIdx) {
                expressions.add(groupedFilters.get(minIdx));
            } else {
                if (minIdx != -1)
                    expressions.add(groupedFilters.get(minIdx));
                if (maxIdx != -1)
                    expressions.add(groupedFilters.get(maxIdx));
            }

            return expressions;
        }

        public static boolean testAllConditionsOnAttribute(RowExpression rowExpression, String name) {
            if (rowExpression instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) rowExpression;
                List<RowExpression> arguments = callExpression.getArguments();

                if (arguments.size() == 2) {
                    if (arguments.get(0) instanceof VariableReferenceExpression && arguments.get(1) instanceof ConstantExpression) {
                        VariableReferenceExpression variableReferenceExpression = (VariableReferenceExpression) arguments.get(0);
                        if (variableReferenceExpression.getName().equals(name)) {
                            System.out.println("filter is on " + name);
                            return true;
                        }
                    }

                    return false;
                } else {
                    throw new UnsupportedOperationException("Need to extend for " + rowExpression.getClass().toString());
                }
            } else {
                throw new UnsupportedOperationException("Need to extend for expressions with more arguments");
            }
        }

        //avoid duplicate expressions
        public static RowExpression eliminateDuplicatesInConjunction(RowExpression rowExpression) {
            List<RowExpression> stack = new ArrayList<>();
            stack.add(rowExpression);

            Set<String> expressionsSeen = new HashSet<>();
            RowExpression ret = null;

            while (!stack.isEmpty()) {
                RowExpression next = stack.get(stack.size() - 1);
                stack.remove(stack.size() - 1);

                if (next instanceof CallExpression) {
                    if (!expressionsSeen.contains(next.toString())) {
                        expressionsSeen.add(next.toString());

                        if (ret == null) {
                            ret = next;
                        } else {
                            List<RowExpression> arguments = new ArrayList<>();
                            arguments.add(next);
                            arguments.add(ret);

                            ret = new SpecialForm(SpecialForm.Form.AND, BooleanType.BOOLEAN, arguments);
                        }
                    }
                }

                if (next instanceof SpecialForm) {
                    SpecialForm specialForm = (SpecialForm) next;

                    if (specialForm.getForm() != SpecialForm.Form.AND) {
                        throw new UnsupportedOperationException("Breaks method contract");
                    }

                    stack.addAll(specialForm.getArguments());
                }
            }

            return ret;
        }

        //push AND down to get a disjunction all ConditionExpressions
        public static RowExpression getDisjunctiveForm(RowExpression rowExpression) {
            if (rowExpression instanceof CallExpression) {
                return rowExpression;
            } else if (rowExpression instanceof SpecialForm) {
                SpecialForm specialForm = (SpecialForm) rowExpression;

                if (specialForm.getForm() == SpecialForm.Form.OR) {
                    List<RowExpression> children = specialForm
                            .getArguments()
                            .stream()
                            .map(expression -> getDisjunctiveForm(expression))
                            .collect(Collectors.toList());

                    SpecialForm ret = new SpecialForm(specialForm.getForm(), specialForm.getType(), children);

                    //System.out.println(ret.toString());

                    System.out.println(specialForm.toString());
                    System.out.println("lhs: " + children.get(0).toString());
                    System.out.println("rhs: " + children.get(1).toString());
                    System.out.println(ret.toString());

                    return ret;
                } else if (specialForm.getForm() == SpecialForm.Form.AND) {
                    List<RowExpression> children = specialForm
                            .getArguments()
                            .stream()
                            .map(expression -> getDisjunctiveForm(expression))
                            .collect(Collectors.toList());

                    List<RowExpression> lhs = decomposeDisjunctiveClause(children.get(0));
                    List<RowExpression> rhs = decomposeDisjunctiveClause(children.get(1));

                    RowExpression ret = null;

                    for (RowExpression le : lhs) {
                        for (RowExpression re : rhs) {
                            List<RowExpression> arguments = new ArrayList<>();
                            arguments.add(le);
                            arguments.add(re);

                            RowExpression pair = new SpecialForm(SpecialForm.Form.AND, specialForm.getType(), arguments);
                            pair = eliminateDuplicatesInConjunction(pair);

                            if (ret == null) {
                                ret = pair;
                            } else {
                                List<RowExpression> retArguments = new ArrayList<>();
                                retArguments.add(pair);
                                retArguments.add(ret);

                                ret = new SpecialForm(SpecialForm.Form.OR, specialForm.getType(), retArguments);
                            }
                        }
                    }

                    //System.out.println(specialForm.toString());
                    //System.out.println("lhs: " + children.get(0).toString());
                    //System.out.println("rhs: " + children.get(1).toString());
                    //System.out.println(ret.toString());

                    //System.out.println(ret.toString());

                    return ret;
                } else {
                    throw new UnsupportedOperationException("not implemented");
                }
            } else {
                throw new UnsupportedOperationException("not implemented");
            }
        }

        public static List<RowExpression> decomposeDisjunctiveClause(RowExpression rowExpression) {
            if (rowExpression instanceof SpecialForm) {
                SpecialForm specialForm = (SpecialForm) rowExpression;

                if (specialForm.getForm() == SpecialForm.Form.OR) {
                    List<RowExpression> arguments = specialForm.getArguments();

                    List<RowExpression> ret = new ArrayList<>();

                    for (RowExpression argument : arguments) {
                        ret.addAll(decomposeDisjunctiveClause(argument));
                    }

                    return ret;
                }
            }

            List<RowExpression> ret = new ArrayList<>();
            ret.add(rowExpression);
            return ret;
        }

        //group predicates by attribute
        public static Map<String, List<RowExpression>> groupClausesBy(RowExpression rowExpression) {
            if (rowExpression instanceof SpecialForm) {
                SpecialForm specialForm = (SpecialForm) rowExpression;

                if (specialForm.getForm() == SpecialForm.Form.OR) {
                    throw new UnsupportedOperationException("OR found in conjunctive statements");
                } else if (specialForm.getForm() == SpecialForm.Form.AND) {
                    List<RowExpression> arguments = specialForm.getArguments();

                    Map<String, List<RowExpression>> ret = new HashMap<>();

                    for (RowExpression argument : arguments) {
                        Map<String, List<RowExpression>> partialGroupBy = groupClausesBy(argument);
                        for (Map.Entry<String, List<RowExpression>> entry : partialGroupBy.entrySet()) {
                            if (!ret.containsKey(entry.getKey())) {
                                ret.put(entry.getKey(), new ArrayList<>());
                            }

                            ret.get(entry.getKey()).addAll(entry.getValue());
                        }
                    }

                    return ret;
                } else {
                    throw new UnsupportedOperationException("Unhandled filter type");
                }
            }

            if (!(rowExpression instanceof CallExpression)) {
                throw new UnsupportedOperationException("Unhandled expression type");
            }

            CallExpression callExpression = (CallExpression) rowExpression;
            List<RowExpression> arguments = callExpression.getArguments();

            if (arguments.size() != 2) {
                throw new UnsupportedOperationException("Need to extend for " + callExpression.getClass().toString());
            }

            if (arguments.get(0) instanceof VariableReferenceExpression && arguments.get(1) instanceof ConstantExpression) {
                VariableReferenceExpression variableReferenceExpression = (VariableReferenceExpression) arguments.get(0);

                Map<String, List<RowExpression>> ret = new HashMap<>();
                List<RowExpression> singletonList = new ArrayList<>();
                singletonList.add(rowExpression);
                ret.put(variableReferenceExpression.getName(), singletonList);
                return ret;
            } else {
                throw new UnsupportedOperationException("Unsupported call expression");
            }
        }
    }


    public static class Lineage {
        //useful mappings to carry around
        Map<String, String> attributeToTable;
        Map<String, Type> attributeToType;
        //we support subexpressions that have a union of conjunctive filters (expressed as a list) on each table
        Map<String, List<ConditionExpression>> filterZones;

        String tmpName;

        //attributes that can be accessed
        private List<String> attributes;

        //join in the query
        private List<JoinNode.EquiJoinClause> tableJoins;
        //join order for this query
        private List<JoinNode.EquiJoinClause> tableJoinsOrdered;
        //tables in the query
        private List<String> tableNames;
        //subexpression is partitioned by
        private List<String> partitionKeys;

        //statistics for estimating footprint
        PlanNodeStatsEstimate planNodeStatsEstimate;

        //is createtable result
        private boolean finalized;
        //create table target
        private TableWriterNode.CreateTarget target;
        //is a subexpression that we can express
        private boolean valid;

        public Lineage(boolean valid) {
            this.attributes = new ArrayList<>();
            this.tableJoins = new ArrayList<>();
            this.tableJoinsOrdered = new ArrayList<>();
            this.tableNames = new ArrayList<>();
            this.partitionKeys = new ArrayList<>();
            this.filterZones = new HashMap<>();
            this.attributeToTable = new HashMap<>();
            this.attributeToType = new HashMap<>();
            this.finalized = false;
            this.target = null;
            this.valid = valid;
            this.planNodeStatsEstimate = null;
            this.tmpName = "tmp";
        }

        public Lineage(Lineage lineage) {
            this.attributeToTable = lineage.attributeToTable;
            this.attributeToType = lineage.attributeToType;
            this.filterZones = lineage.filterZones;

            this.tmpName = lineage.tmpName;

            this.attributes = lineage.attributes;

            this.tableJoins = lineage.tableJoins;
            this.tableJoinsOrdered = lineage.tableJoinsOrdered;
            this.tableNames = lineage.tableNames;
            this.partitionKeys = lineage.partitionKeys;

            this.planNodeStatsEstimate = lineage.planNodeStatsEstimate;

            this.finalized = lineage.finalized;
            this.target = lineage.target;
            this.valid = lineage.valid;
        }

        public void finalize(TableWriterNode.WriterTarget target) {
            finalized = true;

            if (target instanceof TableWriterNode.CreateTarget) {
                this.target = ((TableWriterNode.CreateTarget) target);
            } else {
                throw new UnsupportedOperationException("not implemented");
            }
        }

        public double estimateMemoryFootprint() {
            if (planNodeStatsEstimate == null) return 0.0;

            double rowSize = 0.0;

            for (String attribute : attributes) {
                if (!attributeToType.containsKey(attribute)) {
                    throw new UnsupportedOperationException("unknown attribute type");
                }

                Type type = attributeToType.get(attribute);

                if (type instanceof FixedWidthType) {
                    FixedWidthType fixedWidthType = (FixedWidthType) type;
                    rowSize += fixedWidthType.getFixedSize();
                } else {
                    //magic number
                    rowSize += 16.0;
                }
            }

            return rowSize * planNodeStatsEstimate.getOutputRowCount();
        }

        public PlanNodeStatsEstimate getPlanNodeStatsEstimate() {
            return planNodeStatsEstimate;
        }

        public Map<String, String> getAttributeToTable() {
            return attributeToTable;
        }

        public Map<String, Type> getAttributeToType() {
            return attributeToType;
        }

        //add filter attributes which will be materialized for similar queries
        public Set<String> getExtendedAttributes() {
            Set<String> extendedAttributes = new HashSet<>();
            extendedAttributes.addAll(attributes);

            for (Map.Entry<String, List<ConditionExpression>> entry : filterZones.entrySet()) {
                for (ConditionExpression conditionExpression : entry.getValue()) {
                    for (String attribute : conditionExpression.groupedFilters.keySet()) {
                        extendedAttributes.add(attribute);
                    }
                }
            }

            return extendedAttributes;
        }

        public Map<String, Type> getExtendedAttributesWithTypes() {
            Map<String, Type> extendedAttributesWithTypes = new HashMap<>();

            for (String attribute : attributes) {
                extendedAttributesWithTypes.put(attribute, attributeToType.get(attribute));
            }

            for (Map.Entry<String, List<ConditionExpression>> entry : filterZones.entrySet()) {
                for (ConditionExpression conditionExpression : entry.getValue()) {
                    for (String attribute : conditionExpression.groupedFilters.keySet()) {
                        extendedAttributesWithTypes.put(attribute, attributeToType.get(attribute));
                    }
                }
            }

            return extendedAttributesWithTypes;
        }

        public boolean isFinalized() {
            return finalized;
        }

        public boolean isValid() {
            return valid;
        }

        public void setTmpName(String tmpName) {
            this.tmpName = tmpName;
        }

        public String getNamedView() {
            if (target != null)
                return target.getSchemaTableName().getTableName();

            return tmpName;
        }

        public TableWriterNode.CreateTarget getTarget() {
            return target;
        }

        public void setTarget(TableWriterNode.CreateTarget target) {
            this.target = target;
        }

        public Set<String> getLineageFilterAttributes() {
            Set<String> ret = new HashSet<>();

            for (Map.Entry<String, List<ConditionExpression>> zone : filterZones.entrySet()) {
                if (zone.getValue().size() == 0) continue;

                RowExpression disjunction = null;

                for (ConditionExpression expression : zone.getValue()) {
                    RowExpression conjunction = null;

                    Map<String, List<RowExpression>> groupedFilters = expression.groupedFilters;

                    for (Map.Entry<String, List<RowExpression>> groupedFilterEntry : groupedFilters.entrySet()) {
                        ret.add(groupedFilterEntry.getKey());
                    }
                }
            }

            return ret;
        }

        public Optional<RowExpression> getLineageFilter() {
            RowExpression ret = null;

            for (Map.Entry<String, List<ConditionExpression>> zone : filterZones.entrySet()) {
                if (zone.getValue().size() == 0) continue;

                RowExpression disjunction = null;

                for (ConditionExpression expression : zone.getValue()) {
                    RowExpression conjunction = null;

                    Map<String, List<RowExpression>> groupedFilters = expression.groupedFilters;

                    for (Map.Entry<String, List<RowExpression>> groupedFilterEntry : groupedFilters.entrySet()) {
                        for (RowExpression filter : groupedFilterEntry.getValue()) {
                            if (conjunction == null) {
                                conjunction = filter;
                            } else {
                                conjunction = new SpecialForm(SpecialForm.Form.AND, BooleanType.BOOLEAN, ImmutableList.of(filter, conjunction));
                            }
                        }
                    }

                    if (conjunction != null) {
                        if (disjunction == null) {
                            disjunction = conjunction;
                        } else {
                            disjunction = new SpecialForm(SpecialForm.Form.OR, BooleanType.BOOLEAN, ImmutableList.of(conjunction, disjunction));
                        }
                    }
                }

                if (disjunction != null) {
                    if (ret == null) {
                        ret = disjunction;
                    } else {
                        ret = new SpecialForm(SpecialForm.Form.AND, BooleanType.BOOLEAN, ImmutableList.of(ret, disjunction));
                    }
                }
            }

            return (ret != null) ? Optional.of(ret) : Optional.empty();
        }

        boolean equals(Lineage other) {
            if (!isValid() || !other.isValid())
                return false;

            for (String table : other.tableNames) {
                if (!tableNames.contains(table))
                    return false;
            }

            if (tableNames.size() != other.tableNames.size())
                return false;

            for (JoinNode.EquiJoinClause condition : other.tableJoins) {
                if (!tableJoins.contains(condition))
                    return false;
            }

            if (tableJoins.size() != other.tableJoins.size())
                return false;

            for (String partitionKey : partitionKeys) {
                if (!other.partitionKeys.contains(partitionKey))
                    return false;
            }

            return true;
        }

        //check if other can be computed from this
        public boolean testCoverage(Lineage other) {
            for (String table : tableNames) {
                if (!other.tableNames.contains(table))
                    return false;
            }

            for (JoinNode.EquiJoinClause join : tableJoins) {
                boolean found = false;

                for (JoinNode.EquiJoinClause otherJoin : other.tableJoins) {
                    if (join.equals(otherJoin) || join.flip().equals(otherJoin)) {
                        found = true;
                        break;
                    }
                }

                if (!found) return false;
            }

            Set<String> attributesExtendedOther = other.getExtendedAttributes();
            Set<String> attributesExtended = getExtendedAttributes();

            for (String extendedAttribute : attributesExtendedOther) {
                if (!attributesExtended.contains(extendedAttribute))
                    return false;
            }

            for (String table : tableNames) {
                List<ConditionExpression> expressions = filterZones.get(table);
                List<ConditionExpression> expressionsOther = other.filterZones.get(table);

                if (expressions.isEmpty()) {
                    continue;
                }

                if (expressionsOther.isEmpty()) {
                    return false;
                }

                for (ConditionExpression conditionExpression : expressionsOther) {
                    boolean found = false;

                    for (ConditionExpression localConditionExpression : expressions) {
                        if (localConditionExpression.subsumes(conditionExpression)) {
                            found = true;
                            break;
                        }
                    }

                    if (!found) return false;
                }
            }


            return true;
        }

        public List<String> getPartitionKeys() {
            return partitionKeys;
        }

        public List<String> getTableNames() {
            return tableNames;
        }

        public List<JoinNode.EquiJoinClause> getTableJoins() {
            return tableJoins;
        }

        public List<JoinNode.EquiJoinClause> getTableJoinsOrdered() {
            return tableJoinsOrdered;
        }

        public String getUnfilteredDescriptor() {
            String tableDesc = "";

            for (String table : tableNames) {
                tableDesc = tableDesc + "|" + table;
            }

            String joinDesc = "";

            for (JoinNode.EquiJoinClause join : tableJoins) {
                joinDesc = joinDesc + "|" + join.toString();
            }

            String partDesc = "";

            for (String part : partitionKeys) {
                partDesc = partDesc + "|" + part;
            }

            return "(" + tableDesc + "," + joinDesc + "," + partDesc + ")";
        }

        public String getJoinDescriptor() {
            String tableDesc = "";

            for (String table : tableNames) {
                tableDesc = tableDesc + "|" + table;
            }

            String joinDesc = "";

            for (JoinNode.EquiJoinClause join : tableJoins) {
                joinDesc = joinDesc + "|" + join.toString();
            }

            return "(" + tableDesc + "," + joinDesc + ")";
        }

        @Override
        public String toString() {
            String out = "Valid: " + valid + " Cacheable: " + finalized + " ";

            out += "Name: " + getNamedView() + " ";

            out += "Joins: ";

            for (JoinNode.EquiJoinClause entry : tableJoins) {
                out += entry.toString() + " ";
            }

            out += "Filters: ";

            for (Map.Entry<String, List<ConditionExpression>> entry : filterZones.entrySet()) {
                out += entry.getKey() + " " + entry.getValue().size() + " ";
            }

            out += "Partitioned: ";

            for (String partitionKey : partitionKeys) {
                out += partitionKey + " ";
            }

            return out;
        }

        public static Lineage createBaseTable(String table, PlanNodeStatsEstimate planNodeStatsEstimate, Map<String, Type> attributeToType, List<String> attributes, Optional<RowExpression> rowExpression) {
            Lineage lineage = new Lineage(true);
            lineage.planNodeStatsEstimate = planNodeStatsEstimate;
            lineage.tableNames.add(table);
            lineage.attributes.addAll(attributes);

            if (rowExpression.isPresent()) {
                throw new UnsupportedOperationException("expression not accounted for");
            }

            for (String attribute : attributes) {
                lineage.attributeToTable.put(attribute, table);
            }

            for (String attribute : attributeToType.keySet()) {
                lineage.attributeToType.put(attribute, attributeToType.get(attribute));
            }

            lineage.filterZones.put(table, new ArrayList<>());

            Collections.sort(lineage.tableNames);
            Collections.sort(lineage.tableJoins, (a, b) -> a.toString().compareTo(b.toString()));
            Collections.sort(lineage.partitionKeys);

            return lineage;
        }

        public static Lineage createJoin(Lineage lineage1, Lineage lineage2, PlanNodeStatsEstimate planNodeStatsEstimate, List<JoinNode.EquiJoinClause> clauses, Optional<RowExpression> rowExpression, List<String> attributes) {
            Lineage lineage = new Lineage(true);
            lineage.planNodeStatsEstimate = planNodeStatsEstimate;
            lineage.tableNames.addAll(lineage1.tableNames);
            lineage.tableNames.addAll(lineage2.tableNames);
            lineage.tableJoins.addAll(lineage1.tableJoins);
            lineage.tableJoins.addAll(lineage2.tableJoins);
            lineage.tableJoinsOrdered.addAll(lineage2.tableJoinsOrdered);
            lineage.tableJoinsOrdered.addAll(lineage1.tableJoinsOrdered);
            lineage.attributes.addAll(attributes);

            for (Map.Entry<String, List<ConditionExpression>> entry : lineage1.filterZones.entrySet()) {
                lineage.filterZones.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }

            for (Map.Entry<String, List<ConditionExpression>> entry : lineage2.filterZones.entrySet()) {
                lineage.filterZones.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }

            for (Map.Entry<String, String> entry : lineage1.attributeToTable.entrySet()) {
                lineage.attributeToTable.put(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, String> entry : lineage2.attributeToTable.entrySet()) {
                lineage.attributeToTable.put(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Type> entry : lineage1.attributeToType.entrySet()) {
                lineage.attributeToType.put(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Type> entry : lineage2.attributeToType.entrySet()) {
                lineage.attributeToType.put(entry.getKey(), entry.getValue());
            }

            for (JoinNode.EquiJoinClause clause : clauses) {
                //lineage.tableFilters.add(clause.toString());
                lineage.tableJoins.add(clause);
                lineage.tableJoinsOrdered.add(clause);
            }

            if (rowExpression.isPresent()) {
                throw new UnsupportedOperationException("expression not accounted for");
            }

            Collections.sort(lineage.tableNames);
            Collections.sort(lineage.tableJoins, (a, b) -> a.toString().compareTo(b.toString()));
            Collections.sort(lineage.partitionKeys);

            return lineage;
        }

        public static Lineage createFilter(Lineage lineageOld, PlanNodeStatsEstimate planNodeStatsEstimate, RowExpression rowExpression, List<String> attributes) {
            Lineage lineage = new Lineage(true);
            lineage.planNodeStatsEstimate = planNodeStatsEstimate;
            lineage.tableNames.addAll(lineageOld.tableNames);
            lineage.tableJoins.addAll(lineageOld.tableJoins);
            lineage.tableJoinsOrdered.addAll(lineageOld.tableJoinsOrdered);
            lineage.partitionKeys.addAll(lineageOld.partitionKeys);
            lineage.attributes.addAll(attributes);

            for (Map.Entry<String, List<ConditionExpression>> entry : lineageOld.filterZones.entrySet()) {
                lineage.filterZones.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }

            for (Map.Entry<String, String> entry : lineageOld.attributeToTable.entrySet()) {
                lineage.attributeToTable.put(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Type> entry : lineageOld.attributeToType.entrySet()) {
                lineage.attributeToType.put(entry.getKey(), entry.getValue());
            }

            RowExpression normalized = ConditionExpression.getDisjunctiveForm(rowExpression);

            List<RowExpression> expressions = ConditionExpression.decomposeDisjunctiveClause(normalized);
            List<ConditionExpression> partList = new ArrayList<>();

            for (RowExpression clause : expressions) {
                //System.out.println(clause);

                Map<String, List<RowExpression>> grouping = ConditionExpression.groupClausesBy(clause);
                Map<String, List<RowExpression>> newGrouping = new HashMap<>();

                for (Map.Entry<String, List<RowExpression>> entry : grouping.entrySet()) {
                    //System.out.println("Attribute " + entry.getKey());

                    List<RowExpression> optimized = ConditionExpression.optimizeExpressionOnAttribute(entry.getValue());
                    newGrouping.put(entry.getKey(), optimized);
                    for (RowExpression re : optimized) {
                        //System.out.println("Condition " + re.toString());
                    }
                }

                ConditionExpression ce = new ConditionExpression(newGrouping);
                List<ConditionExpression> survivingPartList = new ArrayList<>();
                boolean survived = true;

                for (ConditionExpression conditions : partList) {
                    if (ce.subsumes(conditions)) {
                        System.out.println("subsumes");
                    } else {
                        survivingPartList.add(conditions);

                        if (conditions.subsumes(ce)) {
                            survived = false;
                            break;
                        }
                    }
                }

                if (survived) survivingPartList.add(ce);

                partList = survivingPartList;
            }

            System.out.println(partList.size());

            /*if (rowExpression instanceof SpecialForm) {
                SpecialForm sform = (SpecialForm) rowExpression;
                System.out.println(sform.getForm() + " " + sform.getType().toString());
                for (RowExpression expr : sform.getArguments()) {
                    System.out.println(expr.getClass().toString() + " " + expr.toString());
                }
            }*/

            for (ConditionExpression conditionExpression : partList) {
                if (lineage.tableNames.size() != 1) {
                    throw new UnsupportedOperationException("not handled");
                }

                lineage.filterZones.get(lineage.tableNames.get(0)).add(conditionExpression);
            }

            //CallExpression ce = (CallExpression) rowExpression;

            Collections.sort(lineage.tableNames);
            Collections.sort(lineage.tableJoins, (a, b) -> a.toString().compareTo(b.toString()));
            Collections.sort(lineage.partitionKeys);


            return lineage;
        }

        public static Lineage createPartitioned(Lineage lineageOld, PlanNodeStatsEstimate planNodeStatsEstimate, List<Symbol> partitionKeys, List<String> attributes) {
            Lineage lineage = new Lineage(true);
            lineage.planNodeStatsEstimate = planNodeStatsEstimate;
            lineage.tableNames.addAll(lineageOld.tableNames);
            lineage.tableJoins.addAll(lineageOld.tableJoins);
            lineage.tableJoinsOrdered.addAll(lineageOld.tableJoinsOrdered);
            lineage.partitionKeys.addAll(partitionKeys.stream().map(symbol -> symbol.getName()).collect(Collectors.toList()));
            lineage.attributes.addAll(attributes);

            for (Map.Entry<String, List<ConditionExpression>> entry : lineageOld.filterZones.entrySet()) {
                lineage.filterZones.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }

            for (Map.Entry<String, String> entry : lineageOld.attributeToTable.entrySet()) {
                lineage.attributeToTable.put(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Type> entry : lineageOld.attributeToType.entrySet()) {
                lineage.attributeToType.put(entry.getKey(), entry.getValue());
            }

            Collections.sort(lineage.tableNames);
            Collections.sort(lineage.tableJoins, (a, b) -> a.toString().compareTo(b.toString()));
            Collections.sort(lineage.partitionKeys);

            return lineage;
        }

        public static Lineage createProject(Lineage lineageOld, List<String> attributes) {
            Lineage lineage = new Lineage(true);
            lineage.planNodeStatsEstimate = lineageOld.planNodeStatsEstimate;
            lineage.tableNames.addAll(lineageOld.tableNames);
            lineage.tableJoins.addAll(lineageOld.tableJoins);
            lineage.tableJoinsOrdered.addAll(lineageOld.tableJoinsOrdered);
            lineage.partitionKeys.addAll(lineageOld.partitionKeys);
            lineage.attributes.addAll(attributes);

            for (Map.Entry<String, List<ConditionExpression>> entry : lineageOld.filterZones.entrySet()) {
                lineage.filterZones.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }

            for (Map.Entry<String, String> entry : lineageOld.attributeToTable.entrySet()) {
                lineage.attributeToTable.put(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Type> entry : lineageOld.attributeToType.entrySet()) {
                lineage.attributeToType.put(entry.getKey(), entry.getValue());
            }

            Collections.sort(lineage.tableNames);
            Collections.sort(lineage.tableJoins, (a, b) -> a.toString().compareTo(b.toString()));
            Collections.sort(lineage.partitionKeys);

            return lineage;
        }
    }


}
