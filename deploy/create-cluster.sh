
aws emr create-cluster \
--applications Name=Hadoop \
--ec2-attributes '{"KeyName":"pciuffetti-coherentdigital", "InstanceProfile":"EMR_EC2_DefaultRole", "SubnetId":"subnet-8266ddce", "EmrManagedSlaveSecurityGroup":"sg-03bfd2f84d0f225e6", "EmrManagedMasterSecurityGroup":"sg-03a4b6217ec1be749" }' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.32.0 \
--name CoherenceBot-${1} \
--no-auto-terminate \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--bootstrap-actions Path=s3://coherencebot/deploy/bootstrap/bootstrap.sh,Args=${1},Name=InstallCoherenceBot \
--log-uri s3://coherencebot/logs/ \
--visible-to-all-users \
--region $1 \
--tags '["component=CoherenceBot", "subsystem=EMR", "owner=pciuffetti@coherentdigital.net", "environment=prod"]' \
--instance-groups '[{"InstanceCount": 1, "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 32, "VolumeType": "gp2"}, "VolumesPerInstance": 2}]}, "InstanceGroupType": "MASTER", "InstanceType": "m4.xlarge", "Name": "Master Instance Group" }, {"InstanceCount": 2, "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 32, "VolumeType": "gp2"}, "VolumesPerInstance": 2}] }, "InstanceGroupType": "CORE", "InstanceType": "m4.xlarge", "Name": "Core Instance Group"}]'
