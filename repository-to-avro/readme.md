# repository-to-avro

![](../FIXorchestraLogo.png)

Generates Avro schemas corresponding to the supplied FIX Orchestra repository.

This Avro schema generation creates separate schemas for each FIX *Message*. It is left to the user to decide if they want to create Avro types that union the *Message* schemas.

Schemas are also generated for each FIX Orchestra ```CodeSet```.

Fields are in-lined, distinct schemas are not generated for each ```Field``` type.

Normalisation of ```Components``` and ```Groups``` in distinct schemas is optional. Where schemas are distinct the Avro code generation for Java requires that schema dependencies be imported in the correct order. A schema must be imported before the schema that depends on it.

"Mapped Datatypes" specifying the mapping to Avro types may be provided in the FIX Orchestra "Datatype" elements.
If they are not provided then the mapping is derived from the applicable FIX type.

 Human-readable names are used throughout.

 ## Options for Avro schema generation 

 |option|required?|description|default|
 |------|---------|-----------|-------|
 |--orchestra-file|Y|The path/name of the FIX Orchestra file|N/A|
 |--output-dir|Y|The output directory|N/A|
 |--namespace|Y|The namespace for the generated schema, it is recommended to include a semantic version in the namespace.|N/A|
 |--normalise-groups|N|Normalise ```Groups``` by writing the schemas to separate files.|false|
 |--normalise-components|N|Normalise ```Components``` by writing the schemas to separate files.|false|
  |--append-repo-fix-version-to-namespace|N|Append the FIX protocol version specified in the Orchestra repository file to the namespace|true|
 