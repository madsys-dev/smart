#!/bin/bash
clush -qS -w ${dtx_test_backends} bash `pwd`/../ae/collect/backends/run_backend_wrapper_pmem.sh ./dtx/tatp/tatp_backend `pwd`
