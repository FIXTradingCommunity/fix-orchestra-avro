package io.fixprotocol.orchestra.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.testing.MojoRule;
import org.apache.maven.plugin.testing.WithoutMojo;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class SchemaGeneratorMojoTest {
	
	private static final String MESSAGES = "messages";
	private static final String IO_FIXPROTOCOL_ORCHESTRA_AVRO_V1_FIXLATEST = "/io/fixprotocol/orchestra/avro/v1/fixlatest";
	private File testTextFile;
	private File testOrchestration;
	private File notPreviouslyExistantOutputDirectory;
	private String buildDirectoryName;
	
	@Before 
	public void before() throws IOException{
        this.buildDirectoryName = System.getProperty("buildDirectory");
        assertNotNull( this.buildDirectoryName );
        // generated-sources
        String testFileName = this.buildDirectoryName + "/aTestFile.txt";
        String testOrchestrationName = this.buildDirectoryName + "/test-classes/project-to-test/resources/trade-latest.xml";
        this.testTextFile = new File(testFileName);
        this.testOrchestration = new File(testOrchestrationName);
        this.testTextFile.createNewFile();
        this.testOrchestration.createNewFile();
        this.notPreviouslyExistantOutputDirectory = new File(buildDirectoryName + "/generated-resources");
	}
	
	@After 
	public void after() throws IOException{
		if (this.testTextFile.exists()) {
			this.testTextFile.delete();
		}
		if (this.notPreviouslyExistantOutputDirectory.exists()) {
			this.notPreviouslyExistantOutputDirectory.delete();
		}
	}
	
	@Rule
	public MojoRule rule = new MojoRule() {
		@Override
		protected void before() throws Throwable {
		}

		@Override
		protected void after() {
		}
	};

	@WithoutMojo
	@Test (expected = MojoExecutionException.class)
	public void orchestrationFileDoesNotExist() throws MojoExecutionException {
		SchemaGeneratorMojo schemaGeneratorMojo = new SchemaGeneratorMojo();
		schemaGeneratorMojo.namespace = "io.fixprotocol.orchestra.avro.v1";
		schemaGeneratorMojo.orchestration = new File("notlikely/nope");
		schemaGeneratorMojo.execute();
	}

	@WithoutMojo
	@Test (expected = MojoExecutionException.class)
	public void outputDirIsNotDirectory() throws MojoExecutionException, IOException {
		SchemaGeneratorMojo schemaGeneratorMojo = new SchemaGeneratorMojo();
		schemaGeneratorMojo.namespace = "io.fixprotocol.orchestra.avro.v1";
		schemaGeneratorMojo.orchestration = this.testOrchestration;
		schemaGeneratorMojo.outputDirectory = this.testTextFile;
		schemaGeneratorMojo.execute();
	}
	
	@WithoutMojo
	@Test
	public void outputDirDoesNotExistSoGetsCreated() throws MojoExecutionException {
		SchemaGeneratorMojo schemaGeneratorMojo = new SchemaGeneratorMojo();
		schemaGeneratorMojo.namespace = "io.fixprotocol.orchestra.avro.v1";
		schemaGeneratorMojo.orchestration = this.testOrchestration;
		schemaGeneratorMojo.outputDirectory = this.notPreviouslyExistantOutputDirectory;
		schemaGeneratorMojo.execute();
		assertTrue(this.notPreviouslyExistantOutputDirectory.exists());
	}

	@Test
	public void testCodeGen() throws Exception {
        File pom = new File( "target/test-classes/project-to-test/" );
        assertNotNull( pom );
        assertTrue( pom.exists() );

        SchemaGeneratorMojo generatorMojo = ( SchemaGeneratorMojo ) rule.lookupConfiguredMojo( pom, "schemaGeneration" );
        assertNotNull( generatorMojo );

        String namespace = ( String ) rule.getVariableValueFromObject( generatorMojo, "namespace" );
        assertNotNull( namespace );
        
        generatorMojo.execute();        

        File orchestration = ( File ) rule.getVariableValueFromObject( generatorMojo, "orchestration" );
        assertNotNull( orchestration );
        assertTrue( orchestration.exists() );
        generatorMojo.getLog().info("outputDirectory : " + orchestration.getAbsolutePath() );

        File outputDirectory = ( File ) rule.getVariableValueFromObject( generatorMojo, "outputDirectory" );
        assertNotNull( outputDirectory );
        assertTrue( outputDirectory.exists() );
        generatorMojo.getLog().info("outputDirectory : " + outputDirectory.getAbsolutePath() );
        
        //simple checks to ensure the code generation has run
        String outputDirName = this.buildDirectoryName + "/test-classes/project-to-test/target/generated-resources";
        File outputDir = new File(outputDirName);
        assertTrue(outputDir.exists());
		Set<String> names = new HashSet<String>(Arrays.asList(outputDir.list()));
		assertTrue(names.contains("io"));
		File quickfix = new File(outputDirName+IO_FIXPROTOCOL_ORCHESTRA_AVRO_V1_FIXLATEST);
		names = new HashSet<String>(Arrays.asList(quickfix.list()));
		assertTrue(names.contains("codesets"));
		assertTrue(names.contains(MESSAGES));
		File fields = new File(outputDirName+IO_FIXPROTOCOL_ORCHESTRA_AVRO_V1_FIXLATEST+"/"+MESSAGES);
		assertEquals(25, fields.list().length);
	}
}

