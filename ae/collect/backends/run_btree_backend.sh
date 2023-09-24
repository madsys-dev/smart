#!/bin/bash
clush -qS -w ${btree_test_backends} bash `pwd`/../ae/collect/backends/run_backend_wrapper.sh ./btree/btree_backend `pwd`
