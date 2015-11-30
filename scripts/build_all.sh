echo "Build Stargate"

curdir=$PWD

cd ..

cd stargate-commons
ant
cd ..

cd stargate-client-hdfs
ant
cd ..

cd stargate-server
ant
cd ..

cd $curdir

