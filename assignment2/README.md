

Key value store with log replication via Raft

To build, run `go install github.com/Manishearth/cs733/assignment2/main && go install github.com/Manishearth/cs733/assignment2/kvstore`
followed by `$GOPATH/bin/main`

The standalone tester can be built similarly via  `go install github.com/Manishearth/cs733/assignment2/standalone_tester`, and running the corresponding executable in the directory

Please ensure that the program to be tested is running first, and that the `config.json` from the `standalone_tester` directory is being used.


------

###Usage

Define a config.json file similar to the one in-tree:

```
{
    "path" : "...", // Unused for now
    "servers" : [
        {"Id": 0, "Hostname": "localhost", "ClientPort": 8000, "LogPort": 9000},
        {"Id": 1, "Hostname": "localhost", "ClientPort": 8001, "LogPort": 9001},
        {"Id": 2, "Hostname": "localhost", "ClientPort": 8002, "LogPort": 9002},
        {"Id": 3, "Hostname": "localhost", "ClientPort": 8003, "LogPort": 9003},
        {"Id": 4, "Hostname": "localhost", "ClientPort": 8004, "LogPort": 9004}
    ]
}
```


For now, please keep the `Id`s sequential and the `Hostname` as `localhost` since the current code is only able to spawn things locally. Don't use port `8999`, it's needed internally for a initial RPC handshake.

Now, you may talk to the first server via TCP. The protocol is as follows:

 - `set <key> <exptime> <numbytes>\r\n<value>\r\n` : queues a message to set `key` with value `value` of length `numbytes`. The response to this should be `OK <version>`, where <version> is a unique id for the key.
 - `get <key>\r\n` : Asks for the value corresponding to the key. The response will be of the form `VALUE <numbytes>\r\n<value bytes>\r\n`.
 - `getm <key>\r\n` : Asks for the value and metadata corresponding to the key. The response will be of the form `VALUE <version> <exptime> <numbytes>\r\n<value bytes>\r\n`.
 - `cas <key> <exptime> <version> <numbytes>\r\n<value>\r\n` : queues a message to set `key` with value `value` of length `numbytes` only if the given version matches. The response to this should be `OK <new_version>`.
 - `delete <key>\r\n` : deletes the key. The response should be `DELETED\r\n`
 

 Possible errors:
  - `ERR_INTERNAL`: Something went wrong inside
  - `ERR_VERSION`: The `cas` command failed due to a version mismatch
  - `ERR_NOT_FOUND`: The key doesn't exist
  - `ERR_CMD_ERR`: The input was malformed