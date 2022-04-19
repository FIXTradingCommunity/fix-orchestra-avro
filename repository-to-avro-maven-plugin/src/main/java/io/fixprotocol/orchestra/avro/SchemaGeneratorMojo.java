package io.fixprotocol.orchestra.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "schemaGeneration", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class SchemaGeneratorMojo extends AbstractMojo {

	/**
	 * Location of orchestration file to parse
	 */
	@Parameter(property = "orchestration", required = true)
	protected File orchestration;

	/**
	 * Output Location for generated schema
	 */
	@Parameter(defaultValue = "${project.build.directory}/generated-sources", property = "outputDirectory", required = true)
	protected File outputDirectory;
	
	/**
	 * Determines if Components will be normalised by writing them to separate files
	 */
	@Parameter(property = "normaliseComponents", required = false)
	protected boolean normaliseComponents = false;

	/**
	 * Determines if Groups will be normalised by writing them to separate files
	 */
	@Parameter(property = "normaliseGroups", required = false)
	protected boolean normaliseGroups = false;
	
	/**
	 * Defines the namespace for the generated schema
	 */
	@Parameter(property = "namespace", required = true)
	String namespace;

	/**
	 * Defines if the FIX version from the repository file will be appended to the provided namespace
	 */
	@Parameter(property = "appendRepoFixVersionToNamespace", required = false)
	boolean appendRepoFixVersionToNamespace = true;

	
	public void execute() throws MojoExecutionException {
        if ( orchestration.exists() && orchestration.isFile() ) {
            this.getLog().info(new StringBuilder("Orchestration : ").append(orchestration.getAbsolutePath()).toString());
		} else {
            String errorMsg = new StringBuilder(orchestration.getAbsolutePath()).append(" must exist and be a file.").toString();
            this.getLog().error(errorMsg.toString());
            throw new MojoExecutionException( errorMsg.toString() );
		}
		if ( outputDirectory.exists() && !outputDirectory.isDirectory() ) {
            String errorMsg = new StringBuilder(outputDirectory.getAbsolutePath()).append(" must be a directory.").toString();
            this.getLog().error(errorMsg.toString());
            throw new MojoExecutionException( errorMsg.toString() );
		} else if (!outputDirectory.exists()) {
			outputDirectory.mkdirs();
        }
		this.getLog().info(new StringBuilder("Output Directory : ").append(outputDirectory.getAbsolutePath()).toString());

		final SchemaGenerator generator = new SchemaGenerator();
		generator.setNamespace(namespace);
		generator.setAppendRepoFixVersionToNamespace(appendRepoFixVersionToNamespace);
		generator.setNormaliseComponents(normaliseComponents);
		generator.setNormaliseGroups(normaliseGroups);
		
	    try (FileInputStream inputFile = new FileInputStream(orchestration)) {
			generator.generate(inputFile, outputDirectory);
		} catch (IOException e) {
			throw new MojoExecutionException(e.toString());
		}
	}
}
