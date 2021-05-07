# xbitman

目标是快速，轻量级生产可用的筛选数据库

* *简单*: 提供http/grpc[WIP]接口 只有简单的几个接口 接入简单
* *快速*: 大部分场景 计数纳秒级，筛选毫秒级响应
* *低内存开销*: 磁盘存储只需要极少的内存开销
* *分布式[WIP]*: TODO raft实现分布式


## QUERY [:table/query] 语法:

    {
        "query": {
            "key": "media_type",
            "op": "=",
            "val": 2
        },
        "sort": {
            "key": "media_id",
            "desc": true
        },
        "limit": {
            "start": 0,
            "size": 100
        }
    }

## COUNT [:table/query] 语法:

    {
        "query": {
            "key": "media_type",
            "op": "=",
            "val": 2
        }
    }

### query 支持嵌套 [and]和[or]嵌套
    
    {
        "query": {
            "and": [
                {
                    "key": "media_type",
                    "op": "=",
                    "val": 2
                },
                {
                    "key": "source",
                    "op": ">",
                    "val": 2
                },
                "or": [
                    {
                        "key": "media_id",
                        "op": "!=",
                        "val": "xxxx"
                    },
                    {
                        "key": "media_id",
                        "op": "!=",
                        "val": "aaaa"
                    }
                ]
            ]
        }
    }

## Put [:table/put] 语法:
    {
        "data": 
        [
            {
                "media_id": "aaaaaa",
                "source": "1",
                "media_type": 2
            },
            {
                "media_id": "xxxx",
                "source": "2",
                "media_type": 3
            }
        ]
    }
 
## 创建表 [table/create] 语法:
   
    {
        "name": "media_info",
        "pkey": {
            "key": "media_id",
            "type": 4
        },
        "indexes": [
            {
                "key": "media_type",
                "type": 2
            },
            {
                "key": "language",
                "type": 4
            },
            {
                "key": "domain",
                "type": 4
            },
            {
                "key": "source",
                "type": 2
            },
            {
                "key": "create_time",
                "type": 4
            },
            {
                "key": "update_time",
                "type": 4
            }
        ]
    }

## 表列表 [tables] 

