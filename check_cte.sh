#!/bin/bash
prefix=/home/root1/openLookEng/cte_bqo/bqo-based-cte
rm $prefix/data/cte_added_for.txt

for i in $(ls $prefix/queries/test)
do
        echo $i >> $prefix/data/cte_added_for.txt
	cd $OLK_BIN
        java -jar /home/root1/openLookEng/cte_bqo/hetu-core/presto-cli/target/hetu-cli-1.9.0-SNAPSHOT-executable.jar --server localhost:8081 -f $prefix/queries/test/$i > $prefix/$i.txt
done


