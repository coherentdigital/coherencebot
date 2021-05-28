if [ $# -eq 0 ]
    then
       echo Provide the s3 bucket or hadoop folder containing the crawl.  eg s3://coherencebot
       exit 1
fi
for SEGMENT in $(hadoop fs -ls $1/crawl/segments | cut -b 58-)
do
   echo Indexing $SEGMENT...
   ./nutch index $1/crawl/crawldb/ -linkdb $1/crawl/linkdb/ $SEGMENT -filter -normalize -deleteGone
done
echo Finished reindexing.