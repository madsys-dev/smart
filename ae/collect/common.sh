# Keep them consistent with ae/plot/common.py

export project_directory=~/sds-ae

export rdma_test_backend=optane06
export hashtable_test_backends=optane06,optane07
export btree_test_backends=optane06,optane07,optane08,optane00,optane01,optane03,optane04,optane02
export dtx_test_backends=optane06,optane07

# $client_machine should not be in $rdma_test_backend, $hashtable_test_backends or $dtx_test_backends
# i.e., optane06,optane07 is not allowed.
export client_machine=optane04

export thread_set=(1 8 16 24 32 40 48 56 64 72 80 88 96)
export max_threads=96

export fig4_depth_set=(1 4 8 16 24 32)
export fig4_thread_set=(12 24 36 48 60 72 84 96)

export fig5_thread_set=(1 2 4 6 8 10 12 14 16)
export fig5_zipfian_set=(0 40 80 90 95 99)

# Six elements, with 1-6 machines respectively
export fig7_compute_machine_list=(optane00 optane0[0,1] optane0[0,1,3] optane0[0,1,3,4] optane0[0,1,3,4,2] optane0[0,1,3,4,2,8])
export fig12_compute_machine_list=(optane06 optane0[6,7] optane0[6,7,8] optane0[6,7,8,0] optane0[6,7,8,0,1] optane0[6,7,8,0,1,3] optane0[6,7,8,0,1,3,4] optane0[6,7,8,0,1,3,4,2])

export fig12_thread_set=(1 8 16 24 32 40 48 56 64 72 80 88 94)
export fig12_max_threads=94

export fig13_depth_set=(1 4 8 12 16 20 24 28 32)
export fig14_thread_set=(1 16 32 48 64 80 88 96)

export interval_set=(8 16 32 64 128 256 512 1024 2048)

export ae_data_path=$project_directory+'/ae/raw'
export ae_figure_path=$project_directory+'/ae/figure'
export build_directory=$project_directory+"/build"
export script_directory=$project_directory+"/script"
