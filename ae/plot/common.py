project_directory = '/home/renfeng/sds-ae'

thread_set = [ 1, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96 ]
max_threads = 96

fig4_depth_set = [1, 4, 8, 16, 24, 32]
fig4_thread_set = [12, 24, 36, 48, 60, 72, 84, 96]

fig5_thread_set = [1, 2, 4, 6, 8, 10, 12, 14, 16]
fig5_zipfian_set = ["0", "0.40", "0.80", "0.90", "0.95", "0.99"]

fig12_thread_set = [ 1, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 94 ]
fig13_depth_set = [1, 4, 8, 12, 16, 20, 24, 28, 32]
fig14_thread_set = [1, 16, 32, 48, 64, 80, 88, 96]

ae_data_path = project_directory + '/ae/raw'
ae_figure_path = project_directory + '/ae/figure'
build_directory = project_directory + "/build"
script_directory = project_directory + "/script"
