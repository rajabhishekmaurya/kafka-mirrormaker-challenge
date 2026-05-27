# MM2 source patch

This directory holds the single Java source file that diverges from Apache
Kafka 4.0.0 — `org/apache/kafka/connect/mirror/MirrorSourceTask.java`.

The same file lives in the Kafka fork at
`connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`;
the two must stay in sync. `Dockerfile.mm2` reads from this directory so the
challenge repo can build the enhanced image without checking in the whole
Kafka tree.

See `../README.md` for what was changed and why.
