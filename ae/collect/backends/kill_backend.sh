#!/bin/bash
clush -qS -w ${rdma_test_backend} killall -q test_rdma
clush -qS -w ${hashtable_test_backends} killall -q hashtable_backend
clush -qS -w ${btree_test_backends} killall -q btree_backend
clush -qS -w ${dtx_test_backends} killall -q smallbank_backend
clush -qS -w ${dtx_test_backends} killall -q tatp_backend