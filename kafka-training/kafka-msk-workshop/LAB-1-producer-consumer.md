
# Terminal 1 - Start Consumer:
cd ~/kafka-msk-workshop/java-clients

# Run consumer with default group
mvn exec:java -Dexec.mainClass="com.workshop.kafka.PaymentConsumer"

# Or with custom group
mvn exec:java -Dexec.mainClass="com.workshop.kafka.PaymentConsumer" -Dexec.args="payment-processors-v2"


---

# Terminal 2 - Start Producer:
cd ~/kafka-msk-workshop/java-clients

# Send 10 payments
java -jar target/kafka-msk-clients-1.0.0.jar 10
