# Deploying CoherenceBot

## Make an Archive
From the repo root:

 git archive -o coherencebot.tar.gz HEAD

## Copy to S3

  aws s3 cp coherencebot.tar.gz s3://coherencebot/deploy/
  aws s3 cp --recursive conf s3://coherencebot/deploy/conf/
  aws s3 cp deploy/bootstrap.sh s3://coherencebot/deploy/bootstrap/bootstrap.sh

## Run AWS CLI to Create
Pass in the region to create the cluster in.

  ./deploy/create-cluster.sh us-east-2
