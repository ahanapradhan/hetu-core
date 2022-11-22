#!/bin/bash
rm /home/root1/Documents/bqo-based-cte/data/cte_added_for.txt

for i in $(ls ~/Documents/bqo-based-cte/queries/tpcds-master/)
do
        echo $i >> /home/root1/Documents/bqo-based-cte/data/cte_added_for.txt
	cd $OLK_BIN
        java -jar hetu-cli-1.9.0-SNAPSHOT-executable.jar --server localhost:8080 -f /home/root1/Documents/bqo-based-cte/queries/tpcds-master/$i > ~/Documents/bqo-based-cte/$i.txt
done


