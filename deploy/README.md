# Deploying CoherenceBot

## Make an Archive
 git archive -o coherencebot.tar.gz HEAD

## Copy to S3

  aws s3 cp coherencebot.tar.gz s3://coherencebot/deploy/
  aws s3 cp --recursive conf s3://coherencebot/deploy/conf/
  aws s3 cp bootstrap.sh s3://coherencebot/deploy/bootstrap/bootstrap.sh

## Run AWS CLI to Create
