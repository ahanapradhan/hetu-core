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


package io.prestosql.sql.planner.datapath.globalplanner;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.plan.*;
import io.prestosql.sql.planner.plan.*;

import java.util.HashMap;
import java.util.Map;

public class SymbolsNameAdapter
{
    //postorder traversal
    public static void adaptSymbolNamesToGlobalNaming(PlanNode planRoot, SymbolsNameAdapterPlanVisitor symbolsNameAdapterPlanVisitor)
    {
        if (planRoot == null) {
            return;
        }

        //what does getSources) give as output? immediate children or all nodes under root?
        for (PlanNode child : planRoot.getSources()) {
            adaptSymbolNamesToGlobalNaming(child, symbolsNameAdapterPlanVisitor);
        }

        planRoot.accept(symbolsNameAdapterPlanVisitor, null);
    }

    static class SymbolsNameAdapterPlanVisitor
            extends InternalPlanVisitor<Void, Void>
    {
        Map<String, Symbol> symbolNamesToAbsoluteNames = new HashMap<String, Symbol>();

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            node.symbolNamesToAbsoluteNames = symbolNamesToAbsoluteNames;
            return null;
        }


//        @Override
        public Void visitOutput(OutputNode node, Void context)
        {
            node.symbolNamesToAbsoluteNames = symbolNamesToAbsoluteNames;
            return null;
        }

       // @Override
        public Void visitIndexSource(IndexSourceNode node, Void context)
        {
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                String[] parts = node.getTableHandle().getConnectorHandle().toString().split(" ");
                String tableName = node.getTableHandle().getCatalogName().toString() + "." + parts[0];
                symbolNamesToAbsoluteNames.put(entry.getKey().getName(), new Symbol(TableScanNode.getActualColName(entry.getKey().getName()), tableName, entry.getValue().toString()));
            }
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                String tableName =  node.getTable().getFullyQualifiedName();
                symbolNamesToAbsoluteNames.put(entry.getKey().getName(), new Symbol(TableScanNode.getActualColName(entry.getKey().getName()), tableName, entry.getValue().toString()));

                entry.getKey().tableName = tableName;
                entry.getKey().attributeName = entry.getValue().toString();
            }
            return null;
        }
    }
}
