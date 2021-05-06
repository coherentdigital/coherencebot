#
# Create and initialize a CoherenceBot Cluster
#
# Args:
# $1 - the region, e.g. us-east-2
# $2 - the instance type, e.g. m4.xlarge
# $3 - the bucket for the region, eg coherencebot-eu1
#
# Prerequisites:
# 1. You need to create a bucket within the region and copy the bootstrap.sh script to it.  This bucket will be used for the region's crawldb.
# 2. You need to copy the ssh key from us-east-2 to the destination region
#    aws ec2 import-key-pair --key-name pciuffetti-coherentdigital --public-key-material fileb://$HOME/.ssh/id_rsa_pciuffetti-coherentdigital.pub --region eu-central-1
#

if [ "$#" -ne 3 ]; then
    echo "Usage: ./create-cluster.sh <Region> <InstanceType> <RegionalBucket>"
    exit 1
fi

aws emr create-cluster \
--applications Name=Hadoop \
--ec2-attributes '{"KeyName":"pciuffetti-coherentdigital", "InstanceProfile":"EMR_EC2_DefaultRole"}' \
--service-role EMR_DefaultRole \
--release-label emr-5.32.0 \
--name CoherenceBot-${1} \
--no-auto-terminate \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--bootstrap-actions Path=s3://$3/deploy/bootstrap/bootstrap.sh,Args=${1},Name=InstallCoherenceBot \
--visible-to-all-users \
--region $1 \
--security-configuration "EMR_DefaultRole access to S3" \
--tags '["component=CoherenceBot", "subsystem=EMR", "owner=pciuffetti@coherentdigital.net", "environment=prod"]' \
--instance-groups '[{"InstanceCount": 1, "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 32, "VolumeType": "gp2"}, "VolumesPerInstance": 2}]}, "InstanceGroupType": "MASTER", "InstanceType": "'${2}'", "Name": "Master Instance Group" }, {"InstanceCount": 2, "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 32, "VolumeType": "gp2"}, "VolumesPerInstance": 2}] }, "InstanceGroupType": "CORE", "InstanceType": "'${2}'", "Configurations":[{"Classification":"mapred-site","Properties":{"mapreduce.map.memory.mb":"2048","yarn.app.mapreduce.am.command-opts":"-Xmx4096m","mapreduce.reduce.memory.mb":"4096","mapreduce.map.skip.maxrecords":"Long.MAX_VALUE","yarn.app.mapreduce.am.resource.mb":"4096"}}], "Name": "Core Instance Group"}]'
