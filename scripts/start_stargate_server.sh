echo "Starting Stargate Server"

curdir=$PWD

if [ $# -eq 0 ]; then
    config_json=""
else
    config_json=$(readlink -f $1)
fi

cd ..
cd stargate-server

echo "Given configuration file - $config_json"
java -cp .:dist/lib/*:dist/stargate-server.jar stargate.server.StargateServer $config_json

cd $curdir
