#!/bin/bash

sudo apt-get update
sudo apt-get install -y build-essential
wget https://go.dev/dl/go1.18.8.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.18.8.linux-amd64.tar.gz
rm -rf go1.18.8.linux-amd64.tar.gz
cat << EOF >> /home/ubuntu/.bashrc
export PATH=$PATH:/usr/local/go/bin
EOF