if [ $# -eq 0 ]
  then
    echo "Here are the known segments"
    for SEGMENT in $(hadoop fs -ls /crawl/segments | cut -b 58-)
    do
	    echo $SEGMENT
    done
    echo "Supply the path and name of a segment like this:"
    echo "./reparse-one.sh /crawl/segments/segmentnumber"
    exit 1;
fi
echo Parsing $1...
./nutch parse -Dmapreduce.map.skip.maxrecords=1000 -Dmapreduce.job.reduces=2 -Dmapreduce.reduce.speculative=false -Dmapreduce.map.speculative=false -Dmapreduce.map.output.compress=true -D mapreduce.task.skip.start.attempts=2 -D mapreduce.map.skip.maxrecords=1 $1
