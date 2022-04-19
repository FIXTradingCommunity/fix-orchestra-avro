# Examples

This module contains the following:

* repository-to-avro-example : Example using the generated schema
* repository-to-avro-example-normalised  : Example using the generated schema with normalised Components and Groups written to separate files.

Theese examples demonstrate some differences between schema generation options. It is optional whether to normalise Components and Groups into distinct schemas in their own files.

These examples are not intended to represent realistic schemas for use with Apache Avro. They are simply structural examples. For example, FIX Session fields such as ```BodyLength``` annd ```CheckSum``` and other "Session layer " fields are included in the schemas. These are unlikely to be interesting for Avro cases (unless the use case is to log messages FIXT1.1 FIX sessions).

It is expected that a user will customise the input FIX Orchestra repository file to represent a custom "Rules of Engagement". These examples use a repository consisting of "Trade" category messages.

Please note that the "normalised" example normalises both Components and Groups, these are distinct options.

The "tests" in ```/src/test/java``` and the ```pom.xml``` files may be interesting to compare, as well as the outputs.

Generated schemas artefacts may be used as constituents of other schemas, for example one may elect to use a ```union``` to permit a schema to contain more than one message type. To allow flexibilty according to the users' requirements the repository-to-avro schema generation does not prescribe the containing/envelope schema.
