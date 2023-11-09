#!/bin/bash

sudo apt-get update
sudo apt-get install -y build-essential unzip
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
wget https://go.dev/dl/go1.18.8.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.18.8.linux-amd64.tar.gz
rm -rf go1.18.8.linux-amd64.tar.gz aws awscliv2.zip
cat << EOF >> /home/ubuntu/.bashrc
export PATH=$PATH:/usr/local/go/bin
EOF