

Key value store with log replication via Raft

To build, run `go install github.com/Manishearth/cs733/assignment2/main && go install github.com/Manishearth/cs733/assignment2/kvstore`
followed by `$GOPATH/bin/main`

The standalone tester can be built similarly via  `go install github.com/Manishearth/cs733/assignment2/standalone_tester`, and running the corresponding executable in the directory

Please ensure that the program to be tested is running first, and that the `config.json` from the `standalone_tester` directory is being used. The `standalone_tester` is designed to kill off some servers as well, which is why the particular config is vital for it to work. 

Under moderate load the system gets much slower than it should. I have yet to investigate this.


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


Internally, these are sent to a log replication mechanism based off of Raft. When the log entry is "committed", then a response will be received


###Code structure

The project is split into the following components:

 - `raft`: This contains the implementation of the Raft protocol, as well as the code for most of the peripherals. This could have been split fiuther, but the ability to define methods on a class from anywhere within the package has made the code more centralized around the `Raft` object and easier to reason about
 - `main`: This is the main executable. It will read the config and spawn servers accordingly
 - `kvstore` (package `server`): This is an executable spawned under the hood by `main`. `main` sends the cluster config to these processes via an RPC. The motivation behind this is twofold; it gave  me some more practice with Go's RPC framework, and it provided an easy way to debug the code.
 - `standalone_test`: This test is a standalone executable (not `_test`-like), which can be run against `main` or someone else's code implementing the same interfaces. It tests the following things:
   - Basic getting and setting
   - Conflicting CAS from parallel routines
   - Ability for program to work with one or two (out of 5) follower servers down
   - Inability for program to work with 3 (out of five) follower servers down
   - Getting and setting of binary values

