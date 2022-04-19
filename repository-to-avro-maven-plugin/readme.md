# repository-to-avro-maven-plugin

![](../FIXorchestraLogo.png)

Apache Maven plugin to generate Avro schemas corresponding to the supplied FIX Orchestra repository.

See [repository-to-avro](../repository-to-avro/readme.md) for more information.

## Options for Avro schema generation 

 |option|required?|description|default|
 |------|---------|-----------|-------|
 |orchestration|Y|The path/name of the FIX Orchestra file|N/A|
 |outputDirectory|Y|The output directory|N/A|
 |namespace|Y|The namespace for the generated schema, it is recommended to include a semantic version in the namespace.|N/A|
 |normaliseGroups|N|Normalise ```Groups``` by writing the schemas to separate files.|false|
 |normaliseComponents|N|Normalise ```Components``` by writing the schemas to separate files.|false|
 |appendRepoFixVersionToNamespace|N|Append the FIX protocol version specified in the Orchestra repository file to the namespace|true|
 