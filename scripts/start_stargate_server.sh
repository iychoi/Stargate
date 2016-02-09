echo "Starting Stargate Server"

curdir=$PWD

if [ $# -eq 0 ]; then
    config_json=""
else
    config_json=$(readlink -f $0)
fi

cd ..
cd stargate-server

java -cp .:dist/lib/*:dist/stargate-server.jar stargate.server.StargateServer $config_json

cd $curdir
