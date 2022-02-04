Needs a running zookeeper instance somewhere, you can use docker on localhost.

Eg, start on localhost:

docker run --name some-zookeeper -p 2181:2181 --restart always -d zookeeper