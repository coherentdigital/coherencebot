# The command-line arg $1 contains a region that this CoherenceBot installation is responsible for.
grep -v COHERENCEBOT ~/.bashrc | grep -v IVY_CACHE_ROOT | grep -v JAVA_HOME > ~/.bashrc_new
echo "# COHERENCEBOT_REGION that this cluster is responsible for:" >> ~/.bashrc_new
echo "export COHERENCEBOT_REGION=$1" >> ~/.bashrc_new
echo "export IVY_CACHE_ROOT=/mnt/coherencebot/ivy-cache" >> ~/.bashrc_new
echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64" >> ~/.bashrc_new
mv ~/.bashrc_new ~/.bashrc
source ~/.bashrc

# Install JAVA SDK
sudo amazon-linux-extras enable corretto8
sudo yum clean metadata
sudo yum install -y java-1.8.0-amazon-corretto-devel

# Install ANT
sudo yum install -y ant

# Copy down the coherencebot repo export from S3. Untar it, and build it.
cd /mnt/
mkdir -p coherencebot
mkdir -p coherencebot/ivy-cache
cd coherencebot
aws s3 cp s3://coherencebot/deploy/coherencebot.tar.gz .
tar -xvzf coherencebot.tar.gz
aws s3 cp --recursive s3://coherencebot/deploy/conf/ conf
if command -v ant ; then
    echo "Ant available" > /tmp/install-results.txt
	ant clean runtime
else
    echo "Ant unavailable" > /tmp/install-results.txt
fi