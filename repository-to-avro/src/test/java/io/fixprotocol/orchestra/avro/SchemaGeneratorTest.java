package io.fixprotocol.orchestra.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import picocli.CommandLine;
import picocli.CommandLine.MissingParameterException;

public class SchemaGeneratorTest {

  private static final String IO_FIXPROTOCOL_ORCHESTRA_AVRO_V1 = "io.fixprotocol.orchestra.avro.v1";
private SchemaGenerator generator;

  @BeforeEach
  public void setUp() throws Exception {
    generator = new SchemaGenerator();
  }

  @Test void testTransformStaticFieldNameFIX50SP2() {
      final String testInput = "FIX50SP2";
      final String expectedResult = "FIX50SP2";
      assertEquals(expectedResult, SchemaGeneratorUtil.precedeCapsWithUnderscore(testInput) );
  }
  
  @Test void testTransformStaticFieldNameFIX44() {
      final String testInput = "FIX44";
      final String expectedResult = "FIX44";
      assertEquals(expectedResult, SchemaGeneratorUtil.precedeCapsWithUnderscore(testInput) );
  }
  
  @Test 
  void testOptions() {
	  /*
		@Option(names = { "-o", "--output-dir" }, defaultValue = "target/generated-sources", 
				paramLabel = "OUTPUT_DIRECTORY", description = "The output directory, Default : ${DEFAULT-VALUE}")
		String outputDir = "target/generated-sources";

		@Option(names = { "-i", "--orchestra-file" }, required = true, 
				paramLabel = "ORCHESTRA_FILE", description = "The path/name of the FIX OrchestraFile")
		String orchestraFileName;

		@Option(names = { "--disableBigDecimal" }, defaultValue = "false", fallbackValue = "true", 
				paramLabel = "DISABLE_BIG_DECIMAL", description = "Disable the use of Big Decimal for Decimal Fields, Default : ${DEFAULT-VALUE}")
		boolean isDisableBigDecimal = true;
		*/
	  String outputDir = "somewhere/generated-sources";
	  String input = "target/input";
	  String[] args = { "--orchestra-file", input, "--output-dir", outputDir, "--namespace", IO_FIXPROTOCOL_ORCHESTRA_AVRO_V1};
	  SchemaGenerator.Options options = new SchemaGenerator.Options();
	  new CommandLine(options).parseArgs(args);
	  assertEquals(input, options.orchestraFileName);
	  assertEquals(outputDir, options.outputDir);
  }

  @Test 
  void testOptionDefaults() {
	  String outputDir = "target/generated-sources";
	  String input = "target/input";
	  String[] args = { "--orchestra-file", input, "--namespace", IO_FIXPROTOCOL_ORCHESTRA_AVRO_V1};
	  SchemaGenerator.Options options = new SchemaGenerator.Options();
	  new CommandLine(options).parseArgs(args);
	  assertEquals(input, options.orchestraFileName);
	  assertEquals(outputDir, options.outputDir);
  }
  
  @Test 
  void testOptionShortForm() {
	  String outputDir = "target/generated-sources";
	  String input = "target/input";
	  String[] args = { "-i", input, "-o", outputDir, "-n", IO_FIXPROTOCOL_ORCHESTRA_AVRO_V1};
	  SchemaGenerator.Options options = new SchemaGenerator.Options();
	  new CommandLine(options).parseArgs(args);
	  assertEquals(input, options.orchestraFileName);
	  assertEquals(outputDir, options.outputDir);
  }
  
  @Test
  void testOptionNotProvided() {
	  String outputDir = "target/generated-sources";
	  String[] args = {"--output-dir", outputDir,};
	  SchemaGenerator.Options options = new SchemaGenerator.Options();
	  assertThrows(MissingParameterException.class, () -> new CommandLine(options).parseArgs(args) );
  }

  @Test 
  void testExecute() {
	  String outputDir = "target/generated-sources";
	  String input = "target/input";
	  String[] args = { "--orchestra-file", input};
	  SchemaGenerator.Options options = new SchemaGenerator.Options();
	  new CommandLine(options).execute(args);
	  assertEquals(input, options.orchestraFileName);
	  assertEquals(outputDir, options.outputDir);
  }
  
  
  @Test
  void testExecuteOptionNotProvided() {
	  String outputDir = "target/generated-sources";
	  String[] args = {"--output-dir", outputDir,};
	  SchemaGenerator.Options options = new SchemaGenerator.Options();
	  new CommandLine(options).execute(args);
  }
  
 
  @Test
  public void testGenerateWithNormalise() throws IOException {
	generator.setNamespace(IO_FIXPROTOCOL_ORCHESTRA_AVRO_V1);
	generator.setNormaliseComponents(true);
	generator.setNormaliseGroups(true);
	generator.setAvroStandard(SchemaGenerator.AVRO_V1);
	generator.generate(
            Thread.currentThread().getContextClassLoader().getResource("trade-latest.xml").openStream(),
            new File("target/spec/generated-sources/normalised/latest"));
  }

}

