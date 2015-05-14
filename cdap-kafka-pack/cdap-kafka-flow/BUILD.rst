==============================
Kafka Flowlet Library for CDAP
==============================


Building the Library
====================
The project is a multi-module maven project. It requires Maven 3.1+ and Java SDK 7 to build.


Prerequisites
=============
The library supports both Kafka 0.7.x and 0.8.x. Since artifacts for Kafka 0.7.x are not
available in the Maven central repository, the project includes an artifact jar for Kafka
0.7.x.

You'll need to install the artifact into your local maven repository before building the
library::

    mvn initialize -P install-kafka-0.7


Build and Install Locally
=========================
After installing the ``kafka-0.7.2`` artifact, you can build and install all artifact jars
using::

    mvn clean install
   
or, to install the jars without running the unit-tests, use::

    mvn clean install -DskipTests
