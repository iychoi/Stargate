echo "Running Stargate <- $1"
java -cp dist/lib/*:dist/Stargate.jar edu.arizona.cs.stargate.service.test.GateKeeperServiceTest $1

