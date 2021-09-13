cp ./cli/src/main/resources/json/flink_aggregations_config.json ./flink/src/main/resources/json/flink_aggregations_config.json
java -cp ./cli/target/cli-1.0-SNAPSHOT-jar-with-dependencies.jar com.logicalclocks.cli.HopsCreateJob -jobType FLINK  -sparkJarFilePath  $(pwd)/flink/target/flink-1.0-SNAPSHOT.jar
java -cp ./cli/target/cli-1.0-SNAPSHOT-jar-with-dependencies.jar com.logicalclocks.cli.HopsRunJob -jobType FLINK -jarFilePath $(pwd)/flink/target/flink-1.0-SNAPSHOT.jar

#java -cp ./cli/target/cli-1.0-SNAPSHOT-jar-with-dependencies.jar com.logicalclocks.cli.HopsCreateJob -jobType SPARK -sparkJarFilePath $(pwd)/microbatching/target/microbatching-1.0-SNAPSHOT.jar
#java -cp ./cli/target/cli-1.0-SNAPSHOT-jar-with-dependencies.jar com.logicalclocks.cli.HopsRunJob -jobType SPARK -jarFilePath $(pwd)/microbatching/target/microbatching-1.0-SNAPSHOT.jar

#java -cp ./cli/target/cli-1.0-SNAPSHOT-jar-with-dependencies.jar com.logicalclocks.cli.HopsCreateJob -jobType SPARK -sparkJarFilePath $(pwd)/deltaStreamer/target/deltaStreamer-3.0.0-SNAPSHOT-jar-with-dependencies.jar
#java -cp ./cli/target/cli-1.0-SNAPSHOT-jar-with-dependencies.jar com.logicalclocks.cli.HopsRunJob -jobType SPARK -jarFilePath $(pwd)/deltaStreamer/target/deltaStreamer-3.0.0-SNAPSHOT-jar-with-dependencies.jar
