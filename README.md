**To start zookeeper**
C:\kafka>.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
Zookeeper starts on - 0.0.0.0/0.0.0.0:2181

**To start kafka**
C:\kafka>.\bin\windows\kafka-server-start.bat .\config\server.properties

**To create a topic**
.\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic TestTopic
.\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic NewTopic

**To list the topics**
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

**To produce messages in a topic**
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic TestTopic

**To consumer messages from a topic**
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic TestTopic --from-beginning

**To kill a process**
netstat -ano | findstr :2181
netstat -ano | findstr :9092
taskkill /F /PID <enter-pid-here>