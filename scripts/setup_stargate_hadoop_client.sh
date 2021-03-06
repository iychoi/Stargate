echo "Setting up Stargate Hadoop Client"

HADOOP_LIB_PATH=~/hadoop/lib
curdir=$PWD

cd ..

cd stargate-commons
cp dist/stargate-commons.jar $HADOOP_LIB_PATH
cd ..

cd stargate-client-hdfs
cp dist/stargate-client-hdfs.jar $HADOOP_LIB_PATH
cd ..

cd $HADOOP_LIB_PATH
wget -O jersey-client-1.8.jar http://central.maven.org/maven2/com/sun/jersey/jersey-client/1.8/jersey-client-1.8.jar
wget -O jackson-jaxrs-1.5.2.jar http://central.maven.org/maven2/org/codehaus/jackson/jackson-jaxrs/1.5.2/jackson-jaxrs-1.5.2.jar
wget -O jackson-xc-1.5.2.jar http://central.maven.org/maven2/org/codehaus/jackson/jackson-xc/1.5.2/jackson-xc-1.5.2.jar

cd $curdir

