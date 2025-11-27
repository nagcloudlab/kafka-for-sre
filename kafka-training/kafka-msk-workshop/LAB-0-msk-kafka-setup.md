# Initialize
terraform init

# Preview
terraform plan

# Deploy (20-30 minutes)
terraform apply

# Get outputs
terraform output

# Get secret key
terraform output -raw client_secret_key
```

---

### **Expected Output**
```
Apply complete! Resources: 14 added, 0 changed, 0 destroyed.

Outputs:

bootstrap_brokers_iam = "b-1.kafka-workshop.xxx.kafka.ap-south-1.amazonaws.com:9198,b-2.kafka-workshop.xxx.kafka.ap-south-1.amazonaws.com:9198,b-3.kafka-workshop.xxx.kafka.ap-south-1.amazonaws.com:9198"

client_access_key = "AKIAXXXXXXXXXXXXXXXX"

connection_info = <<EOT

==========================================
MSK CLUSTER READY
==========================================

Bootstrap Servers (IAM):
b-1.kafka-workshop...

EOT

---


# Get current version
CLUSTER_ARN="arn:aws:kafka:ap-south-1:902190101181:cluster/kafka-workshop-cluster/0a332d14-cfb3-459a-9c6a-2d8327bc503e-4"

CURRENT_VERSION=$(aws kafka describe-cluster --cluster-arn $CLUSTER_ARN --query 'ClusterInfo.CurrentVersion' --output text)

echo "Current Version: $CURRENT_VERSION"

# Enable public access
export AWS_DEFAULT_REGION=ap-south-1
aws kafka update-connectivity \
  --cluster-arn $CLUSTER_ARN \
  --current-version $CURRENT_VERSION \
  --connectivity-info '{"PublicAccess":{"Type":"SERVICE_PROVIDED_EIPS"}}'

# Check cluster status
aws kafka describe-cluster \
  --cluster-arn "arn:aws:kafka:ap-south-1:902190101181:cluster/kafka-workshop-cluster/0a332d14-cfb3-459a-9c6a-2d8327bc503e-4" \
  --query 'ClusterInfo.State' \
  --output text  

aws kafka describe-cluster-operation \
  --cluster-operation-arn "arn:aws:kafka:ap-south-1:902190101181:cluster-operation/kafka-workshop-cluster/0a332d14-cfb3-459a-9c6a-2d8327bc503e-4/7bbc416a-a969-437f-9849-ce8cab975c78" \
  --query 'ClusterOperationInfo.OperationState' \
  --output text  


terraform refresh
terraform output bootstrap_brokers_iam

or

aws kafka get-bootstrap-brokers \
  --cluster-arn "arn:aws:kafka:ap-south-1:902190101181:cluster/kafka-workshop-cluster/0a332d14-cfb3-459a-9c6a-2d8327bc503e-4" \
  --query 'BootstrapBrokerStringPublicSaslIam' \
  --output text

aws kafka get-bootstrap-brokers \
  --cluster-arn "arn:aws:kafka:ap-south-1:902190101181:cluster/kafka-workshop-cluster/0a332d14-cfb3-459a-9c6a-2d8327bc503e-4"  


---



{
    "BootstrapBrokerStringSaslIam": "b-2.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9098,b-1.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9098,b-3.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9098",
    "BootstrapBrokerStringPublicSaslIam": "b-2-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198,b-1-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198,b-3-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198"
}


---

terraform output client_access_key
terraform output -raw client_secret_key
aws configure --profile kafka-workshop
aws sts get-caller-identity --profile kafka-workshop

---


# Download Kafka (same version as MSK)
wget https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka
cd kafka

# Download AWS MSK IAM Auth library
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v2.2.0/aws-msk-iam-auth-2.2.0-all.jar -P libs/


export KAFKA_HOME=/Users/nag/kafka-training/kafka-msk-workshop/kafka
export PATH=$PATH:$KAFKA_HOME/bin
export CLASSPATH=$KAFKA_HOME/libs/aws-msk-iam-auth-2.2.0-all.jar

# MSK Bootstrap Servers (Public)
export MSK_BOOTSTRAP="b-2-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198,b-1-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198,b-3-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198"

# AWS Profile
export AWS_PROFILE=kafka-workshop

---

kafka-topics.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config ./client.properties \
  --list

--


# Create a test topic
kafka-topics.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config ./client.properties \
  --create \
  --topic test-connection \
  --partitions 3 \
  --replication-factor 3


# Ensure using admin credentials (not kafka-workshop)
unset AWS_PROFILE

# Create policy document
cat > /tmp/msk-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ClusterConnect",
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:Connect",
                "kafka-cluster:DescribeCluster"
            ],
            "Resource": "arn:aws:kafka:ap-south-1:902190101181:cluster/kafka-workshop-cluster/*"
        },
        {
            "Sid": "TopicAccess",
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:CreateTopic",
                "kafka-cluster:DeleteTopic",
                "kafka-cluster:DescribeTopic",
                "kafka-cluster:AlterTopic",
                "kafka-cluster:ReadData",
                "kafka-cluster:WriteData"
            ],
            "Resource": "arn:aws:kafka:ap-south-1:902190101181:topic/kafka-workshop-cluster/*"
        },
        {
            "Sid": "GroupAccess",
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:AlterGroup",
                "kafka-cluster:DescribeGroup"
            ],
            "Resource": "arn:aws:kafka:ap-south-1:902190101181:group/kafka-workshop-cluster/*"
        }
    ]
}
EOF

# Get policy ARN
POLICY_ARN=$(aws iam list-policies --query "Policies[?PolicyName=='kafka-workshop-msk-client-policy'].Arn" --output text)

echo "Policy ARN: $POLICY_ARN"

# Update policy
aws iam create-policy-version \
  --policy-arn $POLICY_ARN \
  --policy-document file:///tmp/msk-policy.json \
  --set-as-default


# Set profile back
export AWS_PROFILE=kafka-workshop

# Verify
kafka-topics.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config ./client.properties \
  --describe \
  --topic test-connection  

---

# Test producer (Terminal 1)
kafka-console-producer.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --producer.config ./client.properties \
  --topic payment-events

# Type a message and press Enter


# Test consumer (Terminal 2)
kafka-console-consumer.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --consumer.config ./client.properties \
  --topic payment-events \
  --from-beginning