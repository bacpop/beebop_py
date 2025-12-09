#!/bin/sh
#conda activate poppunk_lineage

EXTERNAL_CLUSTER=8
WORK_DIR="/home/athapar/code/beebop_py"
OUT_DIR="${WORK_DIR}/storage/poppunk_output"

cd ${WORK_DIR};

for RANK in 5 10 25 50;

do

poppunk_assign --model-dir ${WORK_DIR}/storage/dbs/GPS_sub_lineages/gpsc${EXTERNAL_CLUSTER}/rank_${RANK} --db ${WORK_DIR}/storage/dbs/GPS_sub_lineages/gpsc${EXTERNAL_CLUSTER} --query ${WORK_DIR}/qfile.txt --output ${OUT_DIR}/test1/gpsc${EXTERNAL_CLUSTER}_query/rank_${RANK}
# poppunk_visualise --model-dir ${WORK_DIR}/storage/dbs/GPS_sub_lineages/gpsc${EXTERNAL_CLUSTER}/rank_${RANK} --ref-db ${WORK_DIR}/storage/dbs/GPS_sub_lineages/gpsc${EXTERNAL_CLUSTER} --output ${OUT_DIR}/test1/gpsc${EXTERNAL_CLUSTER}_query/rank_${RANK}/visualisation --microreact

done
