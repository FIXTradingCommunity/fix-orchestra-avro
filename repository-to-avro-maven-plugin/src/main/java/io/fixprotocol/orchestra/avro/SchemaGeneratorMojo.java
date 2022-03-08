package io.fixprotocol.orchestra.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import io.fixprotocol.orchestra.avro.SchemaGenerator;

@Mojo(name = "codeGeneration", defaultPhase = LifecyclePhase.PROCESS_SOURCES)
public class SchemaGeneratorMojo extends AbstractMojo {

	/**
	 * Location of orchestration file to parse
	 */
	@Parameter(property = "orchestration", required = true)
	protected File orchestration;

	/**
	 * Output Location for generated sources
	 */
	@Parameter(defaultValue = "${project.build.directory}/generated-sources", property = "outputDirectory", required = true)
	protected File outputDirectory;
	
	/**
	 * Determines if FIX Decimal types will be generated using BigDecimal
	 */
	@Parameter(property = "generateBigDecimal", required = false)
	protected boolean generateBigDecimal = false;

	/**
	 * Determines if FIX Session Layer is generated in FIXT11 package
	 */
	@Parameter(property = "generateFixt11Package", required = false)
	boolean  generateFixt11Package  = true;

	/**
	 * Determines if FIX Session Layer is excluded from Code Generation
	 */
	@Parameter(property = "excludeSession", required = false)
	boolean excludeSession = false;
	
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
		generator.setGenerateStringForDecimal(generateBigDecimal);

	    try (FileInputStream inputFile = new FileInputStream(orchestration)) {
			generator.generate(inputFile, outputDirectory);
		} catch (IOException e) {
			throw new MojoExecutionException(e.toString());
		}
	}
}
