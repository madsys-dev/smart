#!/bin/bash

source ae/collect/common.sh
if [ $HOSTNAME != $client_machine ]
then
    echo "You must run this script in $client_machine"
    echo "Hint: you may check out the field 'client_machine' in ae/collect/common.sh"
    exit 1
fi

cd build
bash ../ae/collect/runall.sh
bash ../ae/plot/runall.sh
