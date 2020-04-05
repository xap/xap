[![Build Status](https://travis-ci.org/xap/xap.svg?branch=master)](https://travis-ci.org/xap/xap)

# Welcome to XAP

XAP is a distributed, highly-scalable, In Memory Data Grid.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

### Prerequisities

Running XAP requires Java 6 or later.

Building XAP requires Java 8 or later (The core requires Java 6, but some optional components and integrations depend on features from higher Java version, so building the entire product requires Java 8).

In addition you'll need [Maven](https://maven.apache.org/) and [Scala](https://www.scala-lang.org/) to build the project.

Make sure environment variable `SCALA_HOME` points to the root of your Scala installation.

### Installing

To build the entire product, simply run the `build` script (either `.sh` or `.bat`) located at the root folder. This builds all the modules and generates a product zip file at `/xap-dist/target`. 

If you're interested in building specific modules, switch to the relevant folder and use maven to build that module.

### Hello World

If this is your first experience with XAP, we recommend you check out the **hello-world** example located at the examples folder. Once you're done, check out the [docs](http://docs.gigaspaces.com/) to learn more, and if you have any questions visit our [community forum](http://ask.gigaspaces.org/questions/).

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We're using standard versioning schema (`major.minor.service-pack.build`). The `master` branch reflects the current state, and each release is tagged in git ([tags on this repository](https://github.com/xap/xap/tags)). 

## Authors

Here's the list of [contributors](https://github.com/xap/xap/contributors) who participated in this project.

Note, however, that during our recent efforts to open source XAP, we had to drop the history, so this list only reflect very recent contributors. We'd like to take this opportunity to honor all past GigaSpaces developers whose contribution is now anonymous - we couldn't have done this without you! 

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE.md](LICENSE.md) file for details.

# Additional Resources

### [Download Center](http://www.gigaspaces.com/xap-download)
### [Documentation](http://docs.gigaspaces.com/)
### [Community Forum](http://ask.gigaspaces.org/questions/)
### [Blog](http://blog.gigaspaces.com/)
### [Training](http://www.gigaspaces.com/Training)

