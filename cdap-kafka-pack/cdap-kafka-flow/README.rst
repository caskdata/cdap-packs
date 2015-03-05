==============================
Kafka Flowlet Library for CDAP
==============================

This library contains classes for consuming data from Apache Kafka.
It supports Kafka versions 0.7.x and 0.8.x. If you are unfamiliar with Apache Kafka,
more information is available at the `Apache Kafka Project Site <http://kafka.apache.org>`__.

How to Use the Library
======================
Using this library depends on the version of the Kafka cluster you are consuming from.

Consume from Kafka 0.8.x
------------------------
To consume data against a 0.8.x cluster, add ``cdap-kafka-flow-compat-0.8`` to your
project dependency. For example, in a Maven ``pom.xml``, add::

    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>cdap-kafka-flow-compat-0.8</artifactId>
      <version>0.1.0</version>
    </dependency>
    
Writing the Consumer Flowlet
````````````````````````````
To create a Flowlet that consumes data from Kafka 0.8.x,
create a class that extends from ``co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet``.
You'll need to specify the Key and Payload type when extending. For example::

    public class MyKafkaConsumer extends Kafka08ConsumerFlowlet<String, String> {
      // ...
    }

For details on implementing this class, see the class documentation of the
``Kafka08ConsumerFlowlet``.
    
Consume from Kafka 0.7.x
------------------------
To consume data against a 0.7.x cluster, add ``cdap-kafka-flow-compat-0.7`` to your
project dependency with the classifier ``embedded-kafka``. For example, in a Maven
``pom.xml``, add::

    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>cdap-kafka-flow-compat-0.7</artifactId>
      <version>0.1.0</version>
      <classifier>embedded-kafka</classifier>
    </dependency>

The classifier ``embedded-kafka`` will bring in the artifact with Kafka classes embedded
inside. The embedded Kafka classes are compiled from version ``0.7.2`` againist
``scala-2.8.0``.

Different Scala Version
```````````````````````
If you want to use a Kafka library that compiles against a different Scala version,
you have to use the normal artifact (without the ``embedded-kafka`` classifier),
exclude the Scala dependency and include a dependency to the Kafka version you want to use.

For example, to create a dependency on ``kafka_2.10-0.7.2`` (a version of ``0.7.2`` that
compiles against ``scala-2.10``)::

    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>cdap-kafka-flow-compat-0.7</artifactId>
      <version>0.1.0</version>
      <exclusions>
        <exclusion>
          <groupId>org.scala-lang</groupId>
          <artifactId>scala-library</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka_2.10</artifactId>
      <version>0.7.2</version>
    </dependency>
    
Writing the Consumer Flowlet
````````````````````````````
To create a Flowlet that consumes data from Kafka 0.7.x,
create a class that extends from ``co.cask.cdap.kafka.flow.Kafka07ConsumerFlowlet``.
You'll need to specify the Payload type when extending. For example::

    public class MyKafkaConsumer extends Kafka07ConsumerFlowlet<String> {
      // ...
    }
    
For details on implementing this class, see the class documentation of the
``Kafka07ConsumerFlowlet``.
