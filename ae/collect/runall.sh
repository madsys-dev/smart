function run() {
    echo $1 ' started ...' >> ../ae/raw/history
    date >> ../ae/raw/history
    bash ../ae/collect/$1.sh
    date >> ../ae/raw/history
    echo $1 ' completed ...' >> ../ae/raw/history
}

mkdir -p ../ae/raw
run fig3-scalability
run fig4-owr
run fig5-hashtable-motivation
run fig7-hashtable-scale-out
run fig8-hashtable-breakdown
run fig9-hashtable-latency
run fig10-dtx-scale-up
run fig11-dtx-scale-latency
run fig12abc-btree-scale-up
run fig12def-btree-scale-out
run fig13-thread-count-batch-size
run fig14-hashtable-conflict
run fig14-hashtable-conflict-bc
run table1
