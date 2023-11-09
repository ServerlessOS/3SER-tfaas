# TFaaS

## 版本号规则：
- 正式版本v2.50.x
- 测试版本v2.50.x-SNAPSHOT
## 配置文件：
- singlenode-local为本地mongodb的测试版本
- singlenode为aws的dynamodb的测试版本
- cluster为aws的dynamodb的正式版本

## 启动
```bash
./init_env.sh

./setup_sshkey.sh

aws configure
AWS Access Key ID [None]: your_access_key
AWS Secret Access Key [None]: your_secret
Default region name [None]: us-east-2
Default output format [None]: json
```

还需手动配置common/storage/dynamodb中的your_access_key和your_secret