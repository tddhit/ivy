# ivy
distributed kv storage

## To-do List
- [x] 实现一致性哈希协议
- [ ] 实现RPC通信框架
- [ ] 通信框架支持redis协议
- [ ] 实现Raft协议，通信框架内置RaftNode
- [ ] 实现LSM Tree本地存储引擎

## Architecture Diagram
```
graph TB
WebServer -->|Redis Protocol| ShardingServer
ShardingServer -->|RPC| StorageServer1
ShardingServer -->|RPC| StorageServer2
```
```
graph LR
    PartionA1-. Replication .->PartionA2
    PartionB2-. Replication .-PartionB1
    subgraph StorageServer1
        PartionA1
        PartionB2
    end
    subgraph StorageServer2
        PartionA2
        PartionB1
    end

```
