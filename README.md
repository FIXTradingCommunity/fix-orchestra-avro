# Repository To Avro Tools 

![](FIXorchestraLogo.png)

The repository contains utilities for integration of FIX Orchestra and Apache Avro.

* FIX Orchestra is intended to provide a standard and some reference implementations for machine readable rules of engagement between counterparties. A customisation of the FIX Orchestra specification for a specific Rules of Engagement is know as an "orchestration". FIX Orchestra is intended to be customised.

* [Apache Avro™](https://avro.apache.org/docs/current/index.html) is a data serialization system. One the features it provides is a binary data format that is widely used in [Apache Kafka](https://kafka.apache.org/) integrations. Avro can similarly be used in JMS implementations and other use cases. Avro data serialization uses Avro schemas defined in JSON. 

Since FIX version 5.0 there has been a logical distinction between the FIX transport and the FIX Application messages. FIX *Messages* can be exchanged using diverse transports. The generation of Avro Schemas from a FIX Orchestra repository supports cases where Avro is used for serialisation of FIX *Messages*. Using Avro schemas generated from FIX Orchestra may be challenging. Please see the ```repository-to-avro-examples```.

These challenges can be mitigated by customising the FIX Orchestration to reduce its complexity. FIX Orchestra is intended to be customised to adapt the Orchestration to the users' specific requirements and in this case to Avro. Without customisation a full FIX ExecutionReport, for example, won't compile : ``` error: code too large```.  Customisation may include the removal of fields associated with the FIX transport (Session).

This Avro schema generation creates separate schemas for each FIX *Message*. Generated schemas artefacts may be used as constituents of other schemas, for example one may elect to use a union to permit a schema to contain more than one *Message" type. To allow flexibilty according to the users' requirements the repository-to-avro schema generation does not prescribe the containing/envelope schema.

```repository-to-avro``` supports options for the schema generation, please see the readme documents in the child projects for details 

At time of writing the FIX Standard Orchestration needs some changes to work with Avro code generation for Java. This is accomplished with a simple ```xslt``` transformation that can be found in the build. 

```xslt```  may be used for schema customisation but please refer to the FIX Orchestra projects for additional tools.

## Modules

* repository-to-avro : Generates an Avro schema corresponding to the supplied FIX Orchestra repository.

* repository-to-avro-maven-plugin : Faciliates FIX Orchestra repository to Avro Schema generation in [Apache Maven](https://maven.apache.org/) projects.

* repository-to-avro-examples : Examples using the generated schema
    * repository-to-avro-example : Example using the generated schema
    * repository-to-avro-example-normalised  : Example using the generated schema with normalised Components and Groups written to separate files.
