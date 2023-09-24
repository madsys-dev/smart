By default, the RDMA driver allows **at most** 12 median-latency
doorbell registers per RDMA context, which make our 
optimiazation impractial. To remove such restriction, we have hacked the rdma-core library used for our test platform.

You may have to recompile the rdma-core library and preload it when executing the application.

1. Fetch the source code of the latest rdma-core library, and install it to your system.

2. Edit the file `rdma-core/providers/mlx5/mlx5.c`, starting from Line 554. 

```c
static int get_num_low_lat_uuars(int tot_uuars)
{
	char *env;
	int num = 4;

	env = getenv("MLX5_NUM_LOW_LAT_UUARS");
	if (env)
		num = atoi(env);

	if (num < 0)
		return -EINVAL;

	// PLEASE REMOVE THE NEXT LINE
	// num = max(num, tot_uuars - MLX5_MED_BFREGS_TSHOLD); 
	return num;
}
```

3. Recompile the rdma-core library

4. Copy the new `libmlx5.so` to the binary path (i.e., `smart/build/`)

5. Execute our application : 
	```
	LD_PRELOAD=libmlx5.so test_application <params>
	```
	If preloading is unsuccessful, a warning message will be printed to stderr. 
