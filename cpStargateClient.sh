cd stargate-commons
ant
cp dist/stargate-commons.jar ~/hadoop/lib
cd ..

cd stargate-client-hdfs
ant
cp dist/stargate-client-hdfs.jar ~/hadoop/lib
cd ..

cd ~/hadoop/lib
wget -O jersey-client-1.8.jar http://central.maven.org/maven2/com/sun/jersey/jersey-client/1.8/jersey-client-1.8.jar
wget -O jackson-jaxrs-1.9.6.jar http://central.maven.org/maven2/org/codehaus/jackson/jackson-jaxrs/1.9.6/jackson-jaxrs-1.9.6.jar
wget -O jackson-xc-1.9.6.jar http://central.maven.org/maven2/org/codehaus/jackson/jackson-xc/1.9.6/jackson-xc-1.9.6.jar

cd ~

