# xbitman

目标是快速，轻量级生产可用的筛选数据库

* *简单*: 提供http/grpc[WIP]接口 只有简单的几个接口 接入简单
* *快速*: 大部分场景 计数纳秒级，筛选毫秒级响应
* *低内存开销*: 磁盘存储只需要极少的内存开销
* *分布式[WIP]*: TODO raft实现分布式

支持的索引类型[type]:

|类型|枚举值|说明|
|---|---|---|
|`Bool`|1|布尔型|
|`Int`|2|整型|
|`Float`|3|浮点型|
|`String`|4|字符串|
|`Set`|5|字符串集合|
|`Multi`|6|一对多集合 类似子表|

支持的操作[op]:

|操作符号|说明|
|---|---|
|`=`|等于|
|`!=`|不等于|
|`>`|大于|
|`>=`|大于等于|
|`<`|小于|
|`<=`|小于等于|
|`in`|类似sql in ; val值为数组|
|`nin`|类似sql not in ; val值为数组|
|`btw`|在[min,max]之间 ; val值为数组|
|`contains`|集合包含 仅用于索引是集合的情况|
|`ncontains`|集合不包含 仅用于索引是集合的情况|

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

### query 支持一对多的索引[Multi]查询:

    {
        "query": {
            "key": "covers",
            "subKey": "height",
            "op": "=",
            "val": 480
        }
    }

## Put [:table/put] 语法:

    {
        "data": 
        [
            {
                "media_id": "aaaaaa",
                "source": "1",
                "media_type": 2,
                "regions": [
                    "ZH","EN"
                ],
                "covers": [
                    {
                        "cover": "https://xxx.jpg",
                        "cover_id": 13444,
                        "height": 480,
                        "width": 854
                    },
                    {
                        "cover": "https://xxx.jpg",
                        "cover_id": 13445,
                        "height": 880,
                        "width": 1260
                    }
                ],
            },
            {
                "media_id": "xxxx",
                "source": "2",
                "media_type": 3,
                
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
                "key": "regions",
                "type": 5
            },
            {
                "key": "covers",
                "type": 6,
                 "subIndexes": [
                    {
                        "key": "height",
                        "type": 2
                    },
                    {
                        "key": "width",
                        "type": 2
                     },

                ]
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

