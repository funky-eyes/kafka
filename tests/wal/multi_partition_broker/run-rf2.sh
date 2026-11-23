#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail
repo=$(git rev-parse --show-toplevel)
cd "$repo"
./gradlew :core:jar :core:copyDependantLibs :clients:shadowJar --console=plain
project_version=$(awk -F= '$1 == "version" { print $2; exit }' gradle.properties)
scala_version=$(awk -F= '$1 == "scalaVersion" { print $2; exit }' gradle.properties)
scala_binary=$(printf '%s' "$scala_version" | cut -d. -f1,2)
probe_root=$(mktemp -d "$repo/build/multi-wal-rf2.XXXXXX")
mkdir -p "$probe_root/classes" "$probe_root/src"
cp tests/wal/multi_partition_broker/*.java "$probe_root/src/"
# Gate only the test copy of the writer. No test hook is shipped in the Broker.
python3 - "$probe_root" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
source = Path('storage/src/main/java/org/apache/kafka/storage/internals/shared/wal/ParallelRingBlockWriter.java').read_text()
needle = '                io.force();'
if source.count(needle) != 1:
    raise SystemExit('writer force site changed; update this test before proceeding')
(root / 'src/ParallelRingBlockWriter.java').write_text(source.replace(needle, '                WalTestGate.await("data");\n' + needle))
PY
cp_base="$probe_root/classes:$repo/core/build/libs/kafka_${scala_binary}-${project_version}.jar:$repo/storage/build/libs/kafka-storage-${project_version}.jar:$repo/clients/build/libs/kafka-clients-${project_version}.jar:$repo/core/build/dependant-libs-${scala_version}/*"
javac -cp "$cp_base" -d "$probe_root/classes" "$probe_root"/src/*.java
git rev-parse HEAD > "$probe_root/source-head.txt"
git status --short > "$probe_root/worktree-status.txt"
shasum -a 256 "core/build/libs/kafka_${scala_binary}-${project_version}.jar" "storage/build/libs/kafka-storage-${project_version}.jar" \
  "clients/build/libs/kafka-clients-${project_version}.jar" "$probe_root"/src/*.java > "$probe_root/artifacts.sha256"
for backend in nio aio; do
 for lose_before_force in false true; do
  probe_dir=$(mktemp -d "$probe_root/$backend.XXXXXX")
  ports=$(python3 -c 'import socket; s=[socket.socket() for _ in range(3)]; [x.bind(("127.0.0.1",0)) for x in s]; print(*(x.getsockname()[1] for x in s)); [x.close() for x in s]')
  read -r controller_port broker1_port broker2_port <<< "$ports"
  cat > "$probe_dir/server-0.properties" <<CONFIG
process.roles=controller
node.id=0
controller.quorum.voters=0@127.0.0.1:$controller_port
listeners=CONTROLLER://127.0.0.1:$controller_port
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$probe_dir/log-0
CONFIG
  for node in 1 2; do
    if [ "$node" = 1 ]; then broker_port=$broker1_port; else broker_port=$broker2_port; fi
    cat > "$probe_dir/server-$node.properties" <<CONFIG
process.roles=broker
node.id=$node
controller.quorum.voters=0@127.0.0.1:$controller_port
listeners=PLAINTEXT://127.0.0.1:$broker_port
advertised.listeners=PLAINTEXT://127.0.0.1:$broker_port
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$probe_dir/log-$node
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
transaction.state.log.num.partitions=1
num.io.threads=2
num.partitions=2
log.cleaner.enable=false
unclean.leader.election.enable=false
storage.extension.class=org.apache.kafka.storage.internals.shared.kafka.SharedStorageExtension
shared.storage.legacy.selectors.enabled=true
shared.storage.topics=parallel-wal-multi
shared.storage.wal.engine=parallel-block
shared.storage.wal.async.partition.append=true
shared.storage.wal.experimental.early.acks.one=true
shared.storage.wal.io.backend=$backend
shared.storage.wal.capacity.bytes=16777216
shared.storage.wal.dir=$probe_dir/wal-$node
CONFIG
  done
  cluster_id=$(java -cp "$cp_base" kafka.tools.StorageTool random-uuid)
  for node in 0 1 2; do
    java -cp "$cp_base" kafka.tools.StorageTool format -t "$cluster_id" -c "$probe_dir/server-$node.properties"
  done
  echo "Running RF2 $backend pre_force_loss=$lose_before_force: $probe_dir"
  java -cp "$cp_base" MultiWalRf2Probe "$probe_dir" "$cp_base" "$broker1_port" "$broker2_port" "$lose_before_force"
 done
done
echo "Evidence: $probe_root"
