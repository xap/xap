# Hello World Example

This example has two counter parts: client and server. The client, a simple main called HelloWorld.java updates a data-grid with "Hello" and "World!" data entities and then reads them back. 
The `HelloWorld` main accepts the following arguments: `-name` {data-grid name} `-mode` {embedded,remote}

## Message.java

A Plain Old Java Object (POJO) is the entity behind the updates to the data-grid. 
It consists of getters and setters for the 'msg' field, and a `@SpaceId` for uniqueness (similar to a Map key).

### Annotations

Additional annotations may be applied - here are a couple:

- A `@SpaceRouting` annotation can be applied to any field to allow routing to partition instances. If not specified, `@SpaceId` will act as the routing field.
- A `@SpaceIndex` annotation can be applied to any field to allow indexing. `@SpaceId` is by default indexed.

## HelloWorld.java

This main class acts as the client. It can either start a single data-grid instance (embedded) in it's JVM, or connect to an existing (remote) data-grid (by it's name).

## Running the Example - Embedded
Import Maven `examples/hello-world/pom.xml` into your IDE of choice as a maven project.
Launch the `HelloWorld` main (arguments: `-name` myDataGrid `-mode` embedded)

This will start an embedded data-grid followed by write and read of Message entities.

### output
```
Created embedded data-grid: myDataGrid
write - 'Hello'
write - 'World!'
read - ['Hello', 'World!']
```

![helloworld-1](images/embedded.png)

## Running the Example - Remote

To connect to a *remote* data-grid, first use the `xap space run` script to launch a data-grid.

From the ${XAP_HOME}/bin directory, run:

-  gs.(sh|bat) space run --lus myDataGrid

Import Maven `examples/hello-world/pom.xml` into your IDE of choice as a maven project.
Launch the `HelloWorld` main (arguments: `-name` myDataGrid `-mode` remote)
> use `myDataGrid` same as the argument passed to `xap space run`

### output
```
Connected to remote data-grid: myDataGrid
write - 'Hello'
write - 'World!'
read - ['Hello', 'World!']
```

![helloworld-1r](images/remote.png)

## Running the Example - Remote (with 2 partitions)

Each partition instance is loaded separately, as follows:

1. Specify `--partitions=2` for two partitions
2. Specify `--instances=1_1` or `--instances=2_1` for each partition instance

From the ${XAP_HOME}/bin directory, run:

-  gs.(sh|bat) space run --lus --partitions=2 **--instances=1_1** myDataGrid
-  gs.(sh|bat) space run --lus --partitions=2 **--instances=2_1** myDataGrid

This will simulate a data-grid of 2 partitioned instances (without backups).

Import Maven `examples/hello-world/pom.xml` into your IDE of choice as a maven project.
Launch the `HelloWorld` main (arguments: `-name` myDataGrid `-mode` remote)

### output
```
Connected to remote data-grid: myDataGrid
write - 'Hello'
write - 'World!'
read - ['Hello', 'World!']
```

![helloworld-2](images/partitioned.png)

## Running the Example - Remote (with backups for each partition)

Each partition instance can be assigned a backup, as follows:

1. Specify `--partitions=2` for two partitions, `--ha` for high availability meaning a single backup for each partition.
2. Specify `--instances=1_1` to load primary of partition id=1, `--instances=1_2` to load the backup instance of partition id=1

**First partition:**

- gs.(sh|bat) space run --lus --partitions=2 --ha **--instances=1_1** myDataGrid
- gs.(sh|bat) space run --lus --partitions=2 --ha **--instances=1_2** myDataGrid

**Second partition:**

-  gs.(sh|bat) space run --lus --partitions=2 --ha **--instances=2_1** myDataGrid
-  gs.(sh|bat) space run --lus --partitions=2 --ha **--instances=2_2** myDataGrid


The Example should be run in the same manner as before - Launch the `HelloWorld` (arguments: `-name` myDataGrid `-mode` remote).

### output
```
Connected to remote data-grid: myDataGrid
write - 'Hello'
write - 'World!'
read - ['Hello', 'World!']
```

![helloworld-3](images/partitioned-with-backup.png)
