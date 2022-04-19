package io.fixprotocol.orchestra.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.fixprotocol._2020.orchestra.repository.Datatype;
import io.fixprotocol._2020.orchestra.repository.MappedDatatype;
import io.fixprotocol._2020.orchestra.repository.MappedDatatype.Extension;

class SchemaGeneratorUtilAvroFieldMappingTest {

	private static final String A_FIELD_NAME = "AfieldName";

	@Test
	public void testAvroMappingFixBaseTypeInt() {
		Map<String, Datatype> datatypes = new HashMap<>();
		String fixType = SchemaGeneratorUtil.INT_TYPE;
		// test uses FIX BaseType NAME if Mapped Datatype not present
		putDatatype(datatypes, fixType, null);
		String fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(fixType), fieldAvroType);

		//test with Mapped Datatype
		putDatatype(datatypes, fixType, null, SchemaGeneratorUtil.INT_TYPE, null);
		fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(fixType), fieldAvroType);
	}
	
	@Test
	public void testAvroMappingFixBaseTypeFloat() {
		Map<String, Datatype> datatypes = new HashMap<>();
		String fixType = SchemaGeneratorUtil.FLOAT_TYPE;
		// test uses FIX BaseType NAME if Mapped Datatype not present
		putDatatype(datatypes, fixType, null);
		String fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(fixType), fieldAvroType);

		//test with Mapped Datatype
		putDatatype(datatypes, fixType, null, SchemaGeneratorUtil.FLOAT_TYPE, null);
		fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(fixType), fieldAvroType);
	}

	@Test
	public void testAvroMappingFixBaseTypeString() {
		Map<String, Datatype> datatypes = new HashMap<>();
		String fixType = SchemaGeneratorUtil.FIX_STRING_TYPE;
		//test uses FIX BaseType if Mapped Datatype not present 
		putDatatype(datatypes, fixType, null);
		String fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.AVRO_STRING_TYPE), fieldAvroType);

		//test with Mapped Datatype
		putDatatype(datatypes, fixType, null, SchemaGeneratorUtil.AVRO_STRING_TYPE, null);
		fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.AVRO_STRING_TYPE), fieldAvroType);
	}
	
	@Test
	public void testAvroMappingFixTypePrice() {
		Map<String, Datatype> datatypes = new HashMap<>();
		String fixType = SchemaGeneratorUtil.FIX_PRICE_TYPE;
		//test uses FIX BaseType if Mapped Datatype not present 
		putDatatype(datatypes, fixType, SchemaGeneratorUtil.FLOAT_TYPE);
		String fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.FLOAT_TYPE), fieldAvroType);

		//test with Mapped Datatype
		putDatatype(datatypes, fixType, SchemaGeneratorUtil.FLOAT_TYPE, SchemaGeneratorUtil.AVRO_DOUBLE_TYPE, null);
		fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.AVRO_DOUBLE_TYPE), fieldAvroType);
	}
	
	@Test
	public void testAvroMappingFixTypePriceAlternative() {
		Map<String, Datatype> datatypes = new HashMap<>();
		String fixType = SchemaGeneratorUtil.FIX_PRICE_TYPE;
		//test uses FIX BaseType if Mapped Datatype not present 
		putDatatype(datatypes, fixType, SchemaGeneratorUtil.FLOAT_TYPE);
		String fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.FLOAT_TYPE), fieldAvroType);

		//test with Mapped Datatype
		putDatatype(datatypes, fixType, SchemaGeneratorUtil.FLOAT_TYPE, SchemaGeneratorUtil.AVRO_STRING_TYPE, null);
		fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.AVRO_STRING_TYPE), fieldAvroType);
	}
	
	@Test
	public void testAvroMappingFixTypeBoolean() {
		Map<String, Datatype> datatypes = new HashMap<>();
		String fixType = SchemaGeneratorUtil.FIX_BOOLEAN_TYPE;
		//test uses FIX BaseType if Mapped Datatype not present 
		putDatatype(datatypes, fixType, SchemaGeneratorUtil.FIX_CHAR_TYPE);
		String fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.AVRO_BOOLEAN_TYPE), fieldAvroType);

		//test with Mapped Datatype
		putDatatype(datatypes, fixType, SchemaGeneratorUtil.FIX_CHAR_TYPE, SchemaGeneratorUtil.AVRO_BOOLEAN_TYPE, null);
		fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.AVRO_BOOLEAN_TYPE), fieldAvroType);
	}

	@Test
	public void testAvroMappingFixBaseTypeUtcTimestamp() {
		Map<String, Datatype> datatypes = new HashMap<>();
		String fixType = SchemaGeneratorUtil.FIX_UTC_TIMESTAMP_TYPE;
		//test uses FIX BaseType if Mapped Datatype not present 
		putDatatype(datatypes, fixType, SchemaGeneratorUtil.FIX_STRING_TYPE);
		String fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.AVRO_STRING_TYPE), fieldAvroType);

		//test with Mapped Datatype
		putDatatype(datatypes, fixType, SchemaGeneratorUtil.FIX_STRING_TYPE, SchemaGeneratorUtil.AVRO_STRING_TYPE, null);
		fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.AVRO_STRING_TYPE), fieldAvroType);
	}
	
	@Test
	public void testAvroMappingDefault() {
		Map<String, Datatype> datatypes = new HashMap<>();
		String fixType = "not-likely_VALUE";
		putDatatype(datatypes, fixType, null);
		String fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, fixType, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals(quoteIt(SchemaGeneratorUtil.AVRO_STRING_TYPE), fieldAvroType);
	}
	
	private String quoteIt(String inString) {
		return String.format("\"%s\"", inString);
	}

	@Test
	public void testAvroMappingWithLogicalTypeFixTypePercentage() {
		//Avro Type {"type": "bytes", "logicalType": "decimal", "scale": "2", "precision": "4"}
		Map<String, Datatype> datatypes = new HashMap<>();
		Extension extension = new Extension();
		LogicalType logicalType = new LogicalType();
		logicalType.setName("decimal");
		logicalType.setKeyValues(new ArrayList<KeyValue>(Arrays.asList(newKeyValue("scale", "2"), newKeyValue("precision", "4"))));
		extension.getAny().add(logicalType);
		//test with Mapped Datatype
		putDatatype(datatypes, SchemaGeneratorUtil.FIX_PERCENTAGE_TYPE, SchemaGeneratorUtil.FLOAT_TYPE, SchemaGeneratorUtil.AVRO_BYTES_TYPE, extension);
		String fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, SchemaGeneratorUtil.FIX_PERCENTAGE_TYPE, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"scale\": \"2\", \"precision\": \"4\"}", fieldAvroType);
	}
	
	@Test
	public void testAvroMappingWithLogicalTypeFixTypeUtcDateOnly() {
		//Avro Type {"type": "int", "logicalType": "date"}
		Map<String, Datatype> datatypes = new HashMap<>();
		Extension extension = new Extension();
		LogicalType logicalType = new LogicalType();
		logicalType.setName("date");
		extension.getAny().add(logicalType);
		//test with Mapped Datatype
		putDatatype(datatypes, SchemaGeneratorUtil.FIX_UTC_DATE_ONLY_TYPE, SchemaGeneratorUtil.FIX_STRING_TYPE, SchemaGeneratorUtil.INT_TYPE, extension);
		String fieldAvroType = SchemaGeneratorUtil.getFieldAvroType(A_FIELD_NAME, SchemaGeneratorUtil.FIX_UTC_DATE_ONLY_TYPE, datatypes, SchemaGenerator.AVRO_V1);
		assertEquals("{\"type\": \"int\", \"logicalType\": \"date\"}", fieldAvroType);
	}

	private KeyValue newKeyValue(String key, String value) {
		KeyValue keyValue = new KeyValue();
		keyValue.setKey(key);
		keyValue.setValue(value);
		return keyValue;
	}

	private void putDatatype(Map<String, Datatype> datatypes, String fixType, String fixBaseType) {
		putDatatype(datatypes, fixType, fixBaseType, null, null);
	}
	
	private void putDatatype(Map<String, Datatype> datatypes, String fixType, String fixBaseType, String avroType, Extension extension) {
		Datatype datatype = new Datatype();
		datatype.setName(fixType);
		if (null != fixBaseType) {
			datatype.setBaseType(fixBaseType);
		}
		// add xml MappedDatatype just to ensure there is more than one MappedDatatype in the list
		MappedDatatype xmlDatatype = new MappedDatatype();
		xmlDatatype.setStandard("XML");
		xmlDatatype.setBase("xs:decimal"); 
		datatype.getMappedDatatype().add(xmlDatatype);
		if (null != avroType) {
			MappedDatatype mappedDatatype = newMappedDatatype(avroType);
			if (null != extension) {
				mappedDatatype.setExtension(extension);
			}
			datatype.getMappedDatatype().add(mappedDatatype);
		}
		datatypes.put(fixType, datatype);
	}

	private MappedDatatype newMappedDatatype(String avroType) {
		MappedDatatype mappedDatatype = new MappedDatatype();
		mappedDatatype.setStandard(SchemaGenerator.AVRO_V1);
		mappedDatatype.setBase(avroType);
		return mappedDatatype;
	}
}
