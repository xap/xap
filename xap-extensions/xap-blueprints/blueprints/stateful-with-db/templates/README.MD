## Getting Started
This package contains the script ( demo-start sh/bat ) that automatically builds and deploys both Mirror 
and Space. The Space is configured to initially load data from the configured data source, for exploring 
functionality of InsightEdge and build applications on top of it.
  
In order to properly run scripts of this blueprint, the contents of this zip file must be extracted under 
<GigaSpaces home>/bin directory.

### Prerequisite for running locally
Have a running GigaSpaces Manager, for example:
gs-agent --manager-local

### Running demo-start (sh/bat)
This script will perform the following:
1. Run a local HSQLDB (if blueprint was not generated from UI)
2. Start a container and deploy the Mirror Processing Unit
3. Start containers and deploy Space Processing Units

Open the GigaSpaces Ops Manager to view the deployed Services and Data.