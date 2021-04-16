for SEGMENT in $(hadoop fs -ls /crawl/segments | cut -b 58-)
do
	echo Parsing $SEGMENT...
	./nutch parse -Dmapreduce.map.skip.maxrecords=1000 -Dmapreduce.job.reduces=2 -Dmapreduce.reduce.speculative=false -Dmapreduce.map.speculative=false -Dmapreduce.map.output.compress=true -D mapreduce.task.skip.start.attempts=2 -D mapreduce.map.skip.maxrecords=1 $SEGMENT
done
