if [ $# -eq 0 ]
  then
    echo "Here are the known segments"
    for SEGMENT in $(hadoop fs -ls /crawl/segments | cut -b 58-)
    do
	    echo $SEGMENT
    done
    echo "Supply the path and name of a segment like this:"
    echo "./reindex-one.sh /crawl/segments/segmentnumber"
    exit 1;
fi
echo Indexing $1...
./nutch index /crawl/crawldb/ -linkdb /crawl/linkdb/ $1 -filter -normalize -deleteGone
