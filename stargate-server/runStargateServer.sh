echo "Running Stargate Server <- $1"
java -cp .:dist/lib/*:dist/stargate-server.jar stargate.server.StargateServer $1

