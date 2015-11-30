echo "Build Stargate"

curdir=$PWD

cd ..

cd stargate-commons
ant clean
ant
cd ..

cd stargate-client-hdfs
ant clean
ant
cd ..

cd stargate-server
ant clean
ant
cd ..

cd $curdir

