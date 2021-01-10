# Hello World Example

This example demonstrates the concepts behind GigaSpaces data grid.
 
There are two counter parts: a client and a server. The client, "HelloWorld.java" updates a 
data-grid with "Hello" and "World!" data entities and then reads them back.

The `HelloWorld` main class accepts the following arguments: `-name` {data-grid name} `-mode` {embedded,remote}

## Message.java

A Plain Old Java Object (POJO) is the entity behind the updates to the data-grid. 
It consists of getters and setters for the 'msg' field, and a `@SpaceId` for uniqueness (similar to a Map key).

### Annotations

Additional annotations can be applied - here are a couple:

- A `@SpaceRouting` annotation can be applied to any field to allow routing to partition instances. If not specified, 
`@SpaceId` will act as the routing field.
- A `@SpaceIndex` annotation can be applied to any field to allow indexing. `@SpaceId` is by default indexed.

## HelloWorld.java

This main class can either start a single data-grid instance (embedded) in it's JVM for easy development, or connect
to an existing (remote) data-grid (by specifying its name).

## Embedded Data Grid
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

## Deploying a Remote Data Grid

To connect to a *remote* data-grid, you first need to start a data grid in a local environment.
It is possible to start a standalone data grid instance, but it is easier to use the Service Grid.

### Starting the Service Grid with 1 data-grid instance
To start the service grid locally with a single container use the run the `gs.(sh|bat)` as follows:
- gs.(sh|bat) host run-agent --auto --gsc=1

To deploy a data grid, run:
- gs.(sh|bat) space deploy myDataGrid

![helloworld-1r](images/remote.png)

### Starting the Service Grid with 2 data-grid partitions
The commands differ in the number of containers hosting the data-grid instances.

- gs.(sh|bat) host run-agent --auto --gsc=2
- gs.(sh|bat) space deploy --partitions=2 myDataGrid

![helloworld-2](images/partitioned.png)

### Starting the Service Grid with 2 highly-available data-grid partitions (one backup for each)
- gs.(sh|bat) host run-agent --auto --gsc=4
- gs.(sh|bat) space deploy --partitions=2 --ha myDataGrid

![helloworld-3](images/partitioned-with-backup.png)

## Running the Example - Remote
Now that we have a remote data-grid, we can connect to it.

Import Maven `examples/hello-world/pom.xml` into your IDE of choice as a maven project.
Launch the `HelloWorld` main (arguments: `-name` myDataGrid `-mode` remote)

This will connect your  client to your remote data-grid followed by write and read of Message entities.

### output
```
Connected to remote data-grid: myDataGrid
write - 'Hello'
write - 'World!'
read - ['Hello', 'World!']
```

## Stopping the data grid

Before deploying a new data grid we need to stop the previous processes.

`Ctrl+c` on all gs.(sh|bat) processes

## Starting standalone data grid instance/s (without Service Grid)

Without the Service Grid, you will need to run the following commands from the ${GS_HOME}/bin directory.

### Single data grid instance
-  gs.(sh|bat) space run --lus myDataGrid

### Data grid with 2 partitions

Each partition instance loads separately, as follows:

1. Specify `--partitions=2` for two partitions
2. Specify `--instances=1_1` or `--instances=2_1` for each partition instance

From the ${GS_HOME}/bin directory, run (in 2 seperate terminals):

-  gs.(sh|bat) space run --lus --partitions=2 **--instances=1_1** myDataGrid
-  gs.(sh|bat) space run --lus --partitions=2 **--instances=2_1** myDataGrid

This will simulate a data-grid of 2 partitioned instances (without backups).

### Data grid with 2 highly available partitions (with backups for each partition)

Each partition instance can be assigned a backup, as follows:

1. Specify `--partitions=2` for two partitions, `--ha` for high availability meaning a single backup for each partition.
2. Specify `--instances=1_1` to load primary of partition id=1, `--instances=1_2` to load the backup instance of partition id=1

**First partition:**

- gs.(sh|bat) space run --lus --partitions=2 --ha **--instances=1_1** myDataGrid
- gs.(sh|bat) space run --lus --partitions=2 --ha **--instances=1_2** myDataGrid

**Second partition:**

-  gs.(sh|bat) space run --lus --partitions=2 --ha **--instances=2_1** myDataGrid
-  gs.(sh|bat) space run --lus --partitions=2 --ha **--instances=2_2** myDataGrid
