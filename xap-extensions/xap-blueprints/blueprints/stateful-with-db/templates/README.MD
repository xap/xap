## Getting Started
This package contains a script (demo-start sh/bat) that automatically builds and deploys GigaSpaces with both a Mirror and a Space. The Space is configured to initially load data from the configured data source, so you can explore the functionality of the GigaSpaces product and build applications on top of it.
  
In order to properly run scripts of this blueprint, the contents of this zip file must be extracted under <GigaSpaces home>/bin directory, for example <GigaSpaces home>/bin/stateful-with-db (the contents of the package should be directly underneath, not nested in an additional folder).

### Prerequisite for running locally
You must run a GigaSpaces Manager, for example:
gs-agent --manager-local

### Running demo-start (sh/bat)
This script does the following:

1. Runs a local HSQLDB (if the blueprint was not generated from GigaSpaces Ops Manager)
2. Starts a container and deploys the Mirror service (Processing Unit).
3. Starts containers and deploys the Space services (Processing Units).

Open GigaSpaces Ops Manager to view the deployed services and data.