# Kafka Cluster Local Lab

### Lab: Setting up a Local Kafka Cluster with 3 Brokers and Kafka UI

### Install java if not already installed
```bash
sudo apt update
sudo apt install openjdk-11-jre -y
```

### Step 1: Download and Setup Kafka Directories

```bash
wget https://dlcdn.apache.org/kafka/3.9.1/kafka_2.13-3.9.1.tgz
tar -xzf kafka_2.13-3.9.1.tgz
mv kafka_2.13-3.9.1 kafka1
cp -rf kafka1 kafka2
cp -rf kafka1 kafka3
rm kafka_2.13-3.9.1.tgz
```


### Step 2: Configure Broker 1
nano kafka1/config/server.properties
```properties
broker.id=1
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
log.dirs=/tmp/kafka-logs-1

default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
zookeeper.connect=localhost:2181
```

### Step 3: Configure Broker 2
nano kafka2/config/server.properties
```properties
broker.id=2
listeners=PLAINTEXT://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9093
log.dirs=/tmp/kafka-logs-2

default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
zookeeper.connect=localhost:2181
```

### Step 4: Configure Broker 3
nano kafka3/config/server.properties
```properties
broker.id=3
listeners=PLAINTEXT://localhost:9094
advertised.listeners=PLAINTEXT://localhost:9094
log.dirs=/tmp/kafka-logs-3

default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
zookeeper.connect=localhost:2181
```

### Step 5: Start ZooKeeper and Brokers
Open 4 separate terminal windows/tabs.
Terminal 1 - ZooKeeper:
```bash
kafka1/bin/zookeeper-server-start.sh kafka1/config/zookeeper.properties
```
Terminal 2 - Broker 1:
```bash
kafka1/bin/kafka-server-start.sh kafka1/config/server.properties
```
Terminal 3 - Broker 2:
```bash
kafka2/bin/kafka-server-start.sh kafka2/config/server.properties
```
Terminal 4 - Broker 3:
```bash
kafka3/bin/kafka-server-start.sh kafka3/config/server.properties
```

### Step 6: Verify Cluster Health
Check all brokers are registered:
```bash
kafka1/bin/zookeeper-shell.sh localhost:2181 <<< "ls /brokers/ids"
```
Expected output: [1, 2, 3]


### Step 7: Setup Kafka UI
Download and configure:
```bash
mkdir kafka-ui
cd kafka-ui
curl -L https://github.com/provectus/kafka-ui/releases/download/v0.7.2/kafka-ui-api-v0.7.2.jar -o kafka-ui-api-v0.7.2.jar
```
Create configuration file:
```bash
nano application.yml
```
```yaml
kafka:
  clusters:
    - name: local-cluster
      bootstrapServers: localhost:9092,localhost:9093,localhost:9094
```

Start Kafka UI (new terminal):
```bash
cd kafka-ui
java -Dspring.config.additional-location=application.yml \
     --add-opens java.rmi/javax.rmi.ssl=ALL-UNNAMED \
     -jar kafka-ui-api-v0.7.2.jar
```     
Access: http://localhost:8080


