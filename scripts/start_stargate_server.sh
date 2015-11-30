echo "Starting Stargate Server"

curdir=$PWD

cd ..
cd stargate-server

java -cp .:dist/lib/*:dist/stargate-server.jar stargate.server.StargateServer ../scripts/stargate.json

cd $curdir
