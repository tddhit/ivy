# ivy
distributed kv storage

## To-do List
- [ ] sharding server
    - [x] 实现一致性哈希协议
    - [ ] 实现TCP Server
    - [ ] 支持redis协议与客户端通信
    - [ ] 实现RPC机制内部通信
- [ ] storage server  
    - [ ] 实现Raft协议，TCP Server内置RaftNode
    - [ ] 实现LSM Tree本地存储引擎
- [ ] monitor server
