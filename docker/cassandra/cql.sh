#!/bin/bash

echo "================== Help for cqlsh ==================="
echo "DESCRIBE tables             : Prints all tables in the current keyspace"
echo "DESCRIBE keyspaces          : Prints all keyspaces in the current cluster"
echo "DESCRIBE <table_name>       : Prints table detail information"
echo "help                        : for more cqlsh commands"
echo "help [cqlsh command]        : Give information about cqlsh commands"
echo "quit                        : quit"
echo "======================================================"

docker exec -it flink-cassandra cqlsh
