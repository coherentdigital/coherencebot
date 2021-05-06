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