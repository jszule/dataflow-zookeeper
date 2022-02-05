- Needs a running zookeeper instance somewhere, you can use docker on localhost.
Eg, start on localhost:
docker run --name some-zookeeper -p 2181:2181 --restart always -d zookeeper
- you have to create a node in the running docker
  - docker ps -> check running containers
  - docker exec -it <container-id> bash
    - type zkCli.sh, this gives a console, you can create a node like "create /test", "create /test/node", "set /test/node green" -> this fill filter out the "green" text from dataflow

- start a pubsub emulator, create a topic (update the topic name in the dataflow code accordingly)
  - you can publish color names into this topic e.g.: with a python script
- you can create an uberjar with "./gradlew clean jar" command into -> build/lib, which you can run with "java -jar dataflow-zookeeper-1.0-SNAPSHOT.jar com.john.lif.Main" command
