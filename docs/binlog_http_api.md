# TiDB-Binlog HTTP API

## Pump

`PumpIP` is the ip of the Pump server. `8250` is the default port of Pump.

1. Get the current status of Pump instances

    ```shell
    curl http://{PumpIP}:8250/status
    ```

    ```shell
    $curl http://127.0.0.1:8250/status

   {
       "status":{
           "ip-172-16-5-71:8250":{
               "nodeId":"ip-172-16-5-71:8250",
               "host":"172.16.5.71:8250",
               "state":"online",
               "isAlive":false,
               "score":0,
               "label":null,
               "maxCommitTS":412518831548007863,
               "updateTS":412518831338292471
           },
           "ip-172-16-5-74:8250":{
               "nodeId":"ip-172-16-5-74:8250",
               "host":"172.16.5.74:8250",
               "state":"online",
               "isAlive":false,
               "score":0,
               "label":null,
               "maxCommitTS":412518831548007869,
               "updateTS":412518831325184860
           },
           "ip-172-16-5-75:8250":{
               "nodeId":"ip-172-16-5-75:8250",
               "host":"172.16.5.75:8250",
               "state":"online",
               "isAlive":false,
               "score":0,
               "label":null,
               "maxCommitTS":412518831548007875,
               "updateTS":412518831534899704
           }
       },
       "CommitTS":412518831849473230,
       "Checkpoint":{
   
       },
       "ErrMsg":""
   }
    ```

1. Get all metrics of Pump

    ```shell
    curl http://{PumpIP}:8250/metrics
    ```

1. Get the status of all drainers

    ```shell
    curl http://{PumpIP}:8250/drainers
    ```

    ```shell
    $curl http://127.0.0.1:8250/drainers

   [
       {
           "nodeId":"ip-172-16-5-70:8249",
           "host":"172.16.5.70:8249",
           "state":"paused",
           "isAlive":false,
           "score":0,
           "label":null,
           "maxCommitTS":412361808537191540,
           "updateTS":412518474704487076
       }
   ]
    ```
1. Change the Pump status

    `NodeID` is the node id of the Pump server. `Action` is the action to execute[possible values: `pause`, `close`].
    `pause` is equivalent to `pause-pump` in [binlogctl](https://github.com/pingcap/tidb-binlog/tree/master/binlogctl), `close` is equivalent to `offline-pump` in [binlogctl](https://github.com/pingcap/tidb-binlog/tree/master/binlogctl).
    
    ```shell
    curl -X PUT http://{PumpIP}:8250/state/{NodeID}/{Action}
    ```

    ```shell
    $curl -X PUT http://127.0.0.1:8250/state/ip-172-16-5-71:8250/stop
    
    {
        "message":"apply action stop success!",
        "code":200
    }
    ```
   
1. Get the binlog by TS

   ```shell
    curl http://{PumpIP}:8250/debug/binlog/{ts}
   ```
   
   ```shell
   $curl http://127.0.0.1:8250/debug/binlog/412518831548007863
   
    tp:Commit start_ts:412518831548007786 commit_ts:412518831548007863 prewrite_key:"t\200\000\000\000\000\000-\212_i\200\000\000\000\000\000\000\001\003\200\000\000\000\000\teK\003\200\000\000\000\000\007\230L"
    
     GetMvccByEncodedKey:
   ```

1. Start the GC

   ```shell
    curl -X POST http://{PumpIP}:8250/debug/gc/trigger
   ```

## Drainer

 `DrainerIP` is the ip of the Drainer server. `8249` is the default port of Drainer.

1. Get the current status of Drainer

   ```shell
    curl http://{DrainerIP}:8249/status
   ```
   
   ```shell
   $curl http://127.0.0.1:8249/status
   
    {
        "PumpPos":{
            "ip-172-16-5-71:8250":412361808550297954,
            "ip-172-16-5-74:8250":412361808550297951,
            "ip-172-16-5-75:8250":412361808550297952
        },
        "Synced":true,
        "LastTS":412361808537191540,
        "TsMap":""
    }
   ```
   
1. Get all metrics of Drainer

    ```shell
    curl http://{DrainerIP}:8249/metrics
    ```

1. Get the lastest commit ts of Drainer

   ```shell
    curl http://{DrainerIP}:8249/commit_ts
   ```
   
   ```shell
   curl http://{DrainerIP}:8249/commit_ts
   
    {
      "message": "get drainer's latest ts success!",
      "code": 200,
      "data": {
        "ts": 412361808537191540
      }
    }
   ```

1. Change the Drainer status

    `NodeID` is the node id of the Drainer server. `Action` is the action to execute[possible values: `pause`, `close`].
    `pause` is equivalent to `pause-drainer` in [binlogctl](https://github.com/pingcap/tidb-binlog/tree/master/binlogctl), `close` is equivalent to `offline-drainer` in [binlogctl](https://github.com/pingcap/tidb-binlog/tree/master/binlogctl).

    ```shell
    curl -X PUT http://{DrainerIP}:8249/state/{NodeID}/{Action}
    ```

    ```shell
    $curl -X PUT http://127.0.0.1:8249/state/ip-172-16-5-70:8249/stop
    
    {
        "message":"apply action stop success!",
        "code":200
    }
    ```
