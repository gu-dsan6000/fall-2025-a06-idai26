#!/bin/bash

# From your local machine, load cluster configuration
source cluster-config.txt

# Copy your script to master node
scp -i $KEY_FILE problem2.py ubuntu@$MASTER_PUBLIC_IP:~/

# SSH to master node
ssh -i $KEY_FILE ubuntu@$MASTER_PUBLIC_IP

# On the master node:
cd ~/spark-cluster
source cluster-ips.txt

# Run your script on the cluster (IMPORTANT: Use your actual net ID!)
# Replace YOUR-NET-ID with your actual net ID (e.g., abc123)
uv run python ~/problem2.py spark://$MASTER_PRIVATE_IP:7077 --net-id ikd3
