package io.fixprotocol.orchestra.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SchemaGeneratorUtilTest {

	@Test
	public void testFirstCharToLowerCase() {
		assertEquals("anExampleString", SchemaGeneratorUtil.firstCharToLowerCase("AnExampleString"));
		assertEquals("anExampleString", SchemaGeneratorUtil.firstCharToLowerCase("anExampleString"));
		assertEquals("", SchemaGeneratorUtil.firstCharToLowerCase(""));
		assertEquals(" ", SchemaGeneratorUtil.firstCharToLowerCase(" "));
		assertEquals(null, SchemaGeneratorUtil.firstCharToLowerCase(null));
	}
}
