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

import io.prestosql.spi.plan.JoinNode;

import java.util.*;

import io.prestosql.ExceededMemoryLimitException;

public class JoinNodeMetadata
{
    //used in the global plan for simple searching for specifc joins
    Set<String> leftTablesSet;
    Set<String> rightTablesSet;
    Set<String> usedViews;
    Optional<String> currentView;
    JoinNode.EquiJoinClause criteria;
    JoinNode joinNodeRef;
    Map<Integer, Double> querySel = new LinkedHashMap<>();

    public JoinNodeMetadata leftChild = null;
    public JoinNodeMetadata rightChild = null;

    public double outputRowCount;
    public  double leftRowCount;
    public double rightRowCount;

    public JoinNodeMetadata(Set<String> leftTablesSet, Set<String> rightTablesSet, Set<String> usedViews, JoinNode.EquiJoinClause criteria, JoinNode joinNodeRef, double outputRowCount)
    {
        this.leftTablesSet = leftTablesSet;
        this.rightTablesSet = rightTablesSet;
        this.usedViews = usedViews;
        this.currentView = Optional.empty();
        this.criteria = criteria;
        this.joinNodeRef = joinNodeRef;
        this.outputRowCount = outputRowCount;
    }

    public JoinNodeMetadata(Set<String> leftTablesSet, Set<String> rightTablesSet, Set<String> usedViews, String currentView, JoinNode.EquiJoinClause criteria, JoinNode joinNodeRef, double outputRowCount)
    {
        this.leftTablesSet = leftTablesSet;
        this.rightTablesSet = rightTablesSet;
        this.usedViews = usedViews;
        this.currentView = Optional.of(currentView);
        this.criteria = criteria;
        this.joinNodeRef = joinNodeRef;
        this.outputRowCount = outputRowCount;
    }

    public JoinNodeMetadata(Set<String> leftTablesSet, Set<String> rightTablesSet, Set<String> usedViews, JoinNode.EquiJoinClause criteria, JoinNode joinNodeRef, double outputRowCount, double leftRowCount, double rightRowCount, Map<Integer, Double> querySel, JoinNodeMetadata leftChild, JoinNodeMetadata rightChild )
    {
        this.leftTablesSet = leftTablesSet;
        this.rightTablesSet = rightTablesSet;
        this.usedViews = usedViews;
        this.currentView = Optional.empty();
        this.criteria = criteria;
        this.joinNodeRef = joinNodeRef;
        this.outputRowCount = outputRowCount;
        this.leftRowCount = leftRowCount;
        this.rightRowCount = rightRowCount;

        for(Integer key: querySel.keySet()){
            this.querySel.put(key, querySel.get(key));
        }

        if (leftChild != null)
            this.leftChild = new JoinNodeMetadata(leftChild.leftTablesSet, leftChild.rightTablesSet, leftChild.usedViews, leftChild.criteria, leftChild.joinNodeRef, leftChild.outputRowCount, leftChild.leftRowCount, leftChild.rightRowCount, leftChild.querySel, leftChild.leftChild, leftChild.rightChild );

        if (rightChild != null)
            this.rightChild = new JoinNodeMetadata(rightChild.leftTablesSet, rightChild.rightTablesSet, rightChild.usedViews, rightChild.criteria, rightChild.joinNodeRef, rightChild.outputRowCount, rightChild.leftRowCount, rightChild.rightRowCount, rightChild.querySel, rightChild.leftChild, rightChild.rightChild );
    }

    public void addQuerySel(int queryCount, double sel){

        //System.out.println(" VSK: addQuerySel: QuerySel.keySet() "+querySel.keySet()+" this = "+this.toString()+" left = "+leftTablesSet+" right = "+rightTablesSet);
        if (!querySel.containsKey(queryCount)){
            querySel.put(queryCount, sel);
        }
//        else {
//            throw new ArithmeticException(" It is not a arithmetic exception; just using it as code should not come here");
//        }
    }

    public void updateRowCountWithChangeInSelectivity(){

        double oldRowCount = outputRowCount;

        if (querySel.size() >=2 ) {
            double newSel = 1;
            for (Integer key : querySel.keySet()) {
                newSel *= (1 - querySel.get(key));
            }
            newSel = 1 - newSel;

            double oldSel = 1;
            int i = 0;
            for (Integer key : querySel.keySet()) {
                if (i < (querySel.size() - 1))
                    oldSel *= (1 - querySel.get(key));
                i++;
            }
            oldSel = 1 - oldSel;

            if (newSel < oldSel)
                newSel = oldSel;


            assert (newSel >= oldSel) : "this cannot happen";

            if (newSel >= (double) 1.0 )
                newSel = 1.0;
            if (oldSel >= (double) 1.0)
                oldSel = 1.0;

            outputRowCount = (oldRowCount) * (newSel / oldSel);
            System.out.println(" updateRowCountWithChangeInSelectivity: old rowcount = "+oldRowCount+" new rowcount = "+outputRowCount);
        }
    }

    public void updateRowCountWithChangeInInputRowCounts(){

        double oldRowCount = outputRowCount;
        double newleftCount = leftRowCount;
        double newRightCount = rightRowCount;

        if(leftChild!=null && (! new Double(leftChild.outputRowCount).isNaN()))
            newleftCount = leftChild.outputRowCount;

        if (rightChild!=null && (! new Double(rightChild.outputRowCount).isNaN()))
            newRightCount = rightChild.outputRowCount;

        outputRowCount = (oldRowCount) * (newleftCount * newRightCount / (leftRowCount * rightRowCount));

        System.out.println(" updateRowCountWithChangeInInputRowCounts: old rowcount = "+oldRowCount+" new rowcount = "+outputRowCount);
    }
}
