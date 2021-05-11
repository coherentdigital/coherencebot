# Deploying CoherenceBot

## Make an Archive
From the repo root, make an archive from the master branch.  Ensure all deltas are checked in:

```
 git archive -o coherencebot.tar.gz HEAD
```

## Copy to S3
```
  aws s3 cp coherencebot.tar.gz s3://coherencebot/deploy/
  aws s3 cp --recursive conf s3://coherencebot/deploy/conf/
  aws s3 cp deploy/bootstrap.sh s3://coherencebot/deploy/bootstrap/bootstrap.sh
```
## Run the AWS CLI to Create a EMR Cluster
Pass in the region to create the cluster in.

```
  ./deploy/create-cluster.sh us-east-2
```

This will use the above bootstrap.sh script to download coherencebot.tar.gz, expand it into /mnt and compile it.  Compilation requires the installation of a Java 8 SDK and Apache Ant.

## Re-deploying CoherenceBot after a PR
- ssh to each cluster.
- Stop the crawl by adding a .STOP file in /mnt/coherencebot/runtime/deploy
- Wait for the crawl to stop; Note: this can take hours for it to finish the iteration it is working on...
- copy down the code and the configuration files from S3.
- Rebuild the code; this will clean out the stop file too.
- Restart the crawl.  Note: this command should reference the S3 bucket for that cluster.

```
  ssh -i "~/.aws/pciuffetti-coherentdigital.pem" hadoop@<emr-master-url>
  cd /mnt/coherencebot
  touch runtime/deploy/.STOP
  tail -f /var/log/coherencebot/coherencebot.log | grep "escaping loop"
  aws s3 cp s3://coherencebot/deploy/coherencebot.tar.gz .
  tar -xvzf coherencebot.tar.gz
  aws s3 cp --recursive s3://coherencebot/deploy/conf/ conf
  cp conf/nutch-site-$COHERENCEBOT_REGION.xml conf/nutch-site.xml
  ant clean runtime
  cd runtime/deploy
  nohup ./bin/crawl --index --num-fetchers 2 s3://coherencebot/crawl -1 >> /var/log/coherencebot/coherencebot.log &
```
