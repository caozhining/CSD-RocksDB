rm -r /mnt/smartssd0/czn/raid_csd_test0
rm -r /mnt/smartssd1/czn/raid_csd_test1
rm -r /mnt/smartssd2/czn/raid_csd_test2
rm -r /mnt/smartssd3/czn/raid_csd_test3

rm -r /mnt/smartssd0/czn/raid_test0
rm -r /mnt/smartssd1/czn/raid_test1
rm -r /mnt/smartssd2/czn/raid_test2
rm -r /mnt/smartssd3/czn/raid_test3

# delete main folder lastly
rm -r /mnt/smartssd3/czn/rocksdb_csd
rm -r /mnt/smartssd3/czn/rocksdb_array_cpu

ops_path="./CPU_ARRAY_16t_ops1.csv"
echo $ops_path
report_path="./CPU_ARRAY16t_top1.txt"
echo $report_path
log_path="./CPU_ARRAY_16t_log1.txt"
echo $log_path
/home/zhining/rocksdb-9.0.0-ARRAY/db_bench --db=/mnt/smartssd3/czn/rocksdb_array_cpu --benchmarks="fillrandom,stats" --num=1000000000 --compression_type=None --key_size=24 --value_size=1024 --format_version=5 --max_background_compactions=16 --max_background_jobs=17 --target_file_size_multiplier=1 --subcompactions=1 --max_background_flushes=-1 --stats_dump_period_sec=120 --open_files=500 --report_interval_seconds=1 --report_file=$ops_path > $log_path &
while true; do
    top -b -n 1 | grep "db_bench" >> $report_path
    sleep 1
done