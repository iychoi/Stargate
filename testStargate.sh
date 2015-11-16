echo "Running Stargate <- $1"
java -cp libs/*:dist/Stargate.jar edu.arizona.cs.stargate.service.test.StargateServiceTest $1

