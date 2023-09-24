#!/bin/bash
clush -qS -w ${rdma_test_backend} bash `pwd`/../ae/collect/backends/run_backend_wrapper.sh ./test/test_rdma `pwd`
