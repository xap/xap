# Scala Example Project

## Overview
This is an example project that shows how XAP Scala can be used in a real project and how Scala and Java code might be integrated.

## XAP Maven plugin project
The project is based on a template project (event-processing) from the XAP Maven plugin. A few changes were introduced:

- `common` module, which implements Space classes, is written in Scala and takes advantage of constructor based properties.
- A new module - `verifier` was introduced. It uses a class with constructor based properties and predicate based queries to obtain objects from Space.
- The Build process of `common` and `verifier` modules was modified to handle Scala and mixed Java/Scala modules, respectively.

## Requirements
- JDK version of at least 1.6 is required to build the project.
- The project uses maven build tool.
- To run the project, Scala libraries have to be a accessible for XAP.
- Scala is not required to build the project, since requried libraries will be downloaded by maven.

## Build -&- Run Steps
- From cloned git repo `xap/xap-extensions/xap-scala` run command: `mvn clean install`
- From the example's main directory `xap/xap-extensions/xap-scala/xap-scala-example`
    - run command: `$XAP_HOME/bin/gs pu run --lus processor/target/xap-scala-example-processor.jar`
    - (in a separate console) run command: `$XAP_HOME/bin/gs pu run --lus feeder/target/xap-scala-example-feeder.jar`
    
The above steps will run the data-grid space (processor) which will process feeds from the feeder.
