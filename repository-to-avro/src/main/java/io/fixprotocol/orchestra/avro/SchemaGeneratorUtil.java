package io.fixprotocol.orchestra.avro;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.fixprotocol._2020.orchestra.repository.Datatype;
import io.fixprotocol._2020.orchestra.repository.Documentation;
import io.fixprotocol._2020.orchestra.repository.FieldRefType;
import io.fixprotocol._2020.orchestra.repository.FieldType;
import io.fixprotocol._2020.orchestra.repository.MappedDatatype;
import io.fixprotocol._2020.orchestra.repository.MappedDatatype.Extension;
import io.fixprotocol._2020.orchestra.repository.PresenceT;

public class SchemaGeneratorUtil {
	public static final String INT_TYPE = "int";
	public static final String FLOAT_TYPE = "float";
	public static final String AVRO_BOOLEAN_TYPE = "boolean";
	public static final String AVRO_STRING_TYPE = "string";
	public static final String AVRO_DOUBLE_TYPE = "double";
	public static final String AVRO_BYTES_TYPE = "bytes";
	
	public static final String FIX_UTC_DATE_ONLY_TYPE = "UTCDateOnly";
	public static final String FIX_UTC_TIMESTAMP_TYPE = "UTCTimestamp";
	public static final String FIX_PERCENTAGE_TYPE = "Percentage";
	public static final String FIX_PRICE_TYPE = "Price";
	public static final String FIX_CHAR_TYPE = "char";
	public static final String FIX_BOOLEAN_TYPE = "Boolean";
	public static final String FIX_STRING_TYPE = "String";
	
    static String precedeCapsWithUnderscore(String stringToTransform) {
        return stringToTransform.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase();
    }

	static String indent(int level) {
		final char[] chars = new char[level * SchemaGenerator.SPACES_PER_LEVEL];
		Arrays.fill(chars, ' ');
		return new String(chars);
	}

	static File getAvroPath(File outputDir, String namespace, String dirName) {
		final StringBuilder sb = new StringBuilder();
		sb.append(namespace.replace('.', File.separatorChar));
		sb.append(File.separatorChar).append(dirName);
		return new File(outputDir, sb.toString());
	}

	static Writer writeOpenBracket(Writer writer) throws IOException {
		writer.write("{\n");
		return writer;
	}

	static Writer writeCloseBracket(Writer writer) throws IOException {
		writer.write("}\n");
		return writer;
	}

	static String getFieldInlineString(FieldRefType fieldRefType, FieldType fieldType, final String name, final String type, int indent) {
		StringBuffer result = new StringBuffer();
		result.append(indent(indent));
		result.append("{");
		result.append(SchemaGeneratorUtil.getJsonNameValue("name", name, true));
		// Note that the type is not quoted here as it is received in the appropriate format  
		if (fieldRefType.getPresence().equals(PresenceT.REQUIRED)) {
			result.append("\"type\": ").append(type).append(",");
		} else {
			StringBuffer optionalFieldTypeString = new StringBuffer("[\"null\", ").append(type).append("]");
			result.append("\"type\": ").append(optionalFieldTypeString.toString()).append(",");
			result.append(" \"default\": null,");
		}
		List<Object> members = fieldRefType.getAnnotation().getDocumentationOrAppinfo();
		List<String> docs = new ArrayList<>();
		getDocumentationStrings(members, docs);
		docs.add("FIX datatype : ".concat(fieldType.getType()));
		members = fieldType.getAnnotation().getDocumentationOrAppinfo();
		getDocumentationStrings(members, docs);
		
		result.append(SchemaGeneratorUtil.getJsonNameValue("doc", String.join(", ", docs).trim(), false));			
		result.append("}");
		return result.toString();
	}

	static void getDocumentationStrings(List<Object> members, List<String> docs) {
		for (Object member : members) {
			if (member instanceof Documentation) {
				((Documentation) member).getContent().forEach(d -> {
					docs.add(d.toString().replace("\n", " ").replace("\"", "\\\"").trim());
				});
			}
		}
	}

	static void writeField(FieldType fieldType, String namespace, final String name, final String type,
			FileWriter writer) throws IOException {
		writeOpenBracket(writer);
		writeName(writer, name, 1);
		writeNameSpace(writer, namespace, SchemaGenerator.FIELD_DIR);
		writeJsonNameValue(writer, indent(1), "type", "record");
		writer.write("\n");
		List<Object> members = fieldType.getAnnotation().getDocumentationOrAppinfo();
		List<String> docs = new ArrayList<>();
		docs.add("FIX datatype : ".concat(fieldType.getType().trim()));
		for (Object member : members) {
			if (member instanceof Documentation) {
				((Documentation) member).getContent().forEach(d -> {
					docs.add(d.toString().trim());
				});
			}
		}
		writeJsonNameValue(writer, indent(1), "doc", String.join(",", docs).trim());
		writer.write("\n");
		
		writeFieldArrayStart(writer);
	
		writeJsonNameValue(writer, indent(3), "name", "value");
		writer.write("\n");
		writeJsonNameValue(writer, indent(3), "type", type, false);
		writer.write("\n");
	
		writeFieldArrayEnd(writer);
		writeCloseBracket(writer);
	}

	static Writer writeName(Writer writer, String name, int indent) throws IOException {
		writeJsonNameValue(writer, indent(indent), "name", name);
		writer.write("\n");
		return writer;
	}

	static Writer writeNameSpace(Writer writer, String namespace, String suffix) throws IOException {
		writeJsonNameValue(writer, indent(1),  "namespace", namespace.concat(".").concat(suffix));
		writer.write("\n");
		return writer;
	}

	static Writer writeFieldArrayStart(Writer writer) throws IOException {
		writer.write(indent(1));
		writer.write("\"fields\": [\n");
		writer.write(indent(2));
		writer.write("{\n");
		return writer;
	}

	static Writer writeFieldArrayEnd(Writer writer) throws IOException {
		writer.write(indent(2));
		writer.write("}\n");
		writer.write(indent(1));
		writer.write("]\n");
		return writer;
	}

	static Writer writeEnumDef(Writer writer) throws IOException {
		writeJsonNameValue(writer, indent(1), "type", "enum");
		writer.write("\n");
		writer.write(indent(1));
		writer.write("\"symbols\": [\n");
		return writer;
	}

	static Writer writeEndEnumDef(Writer writer, String name, String unknown) throws IOException {
		writer.write(indent(1));
		writer.write("],\n");
		writer.write(indent(1));
		writer.write("\"default\": \"");
		writer.write(unknown);
		writer.write("\"\n");
		return writer;
	}

	static Writer writeJsonNameValue(Writer writer, String indent, String name, String value) throws IOException {
		return writeJsonNameValue(writer, indent, name, value, true);
	}

	static Writer writeJsonNameValue(Writer writer, String indent, String name, String value, boolean isWriteTrailingComma) throws IOException {
		writer.write(indent);
		writer.write(getJsonNameValue(name, value, isWriteTrailingComma));
		return writer;
	}

	static String getJsonNameValue(String name, String value, boolean isWriteTrailingComma) {
		StringBuffer result = new StringBuffer();
		result.append("\"");
		result.append(name);
		result.append("\": \"");
		result.append(value);
		result.append("\"");
		if (isWriteTrailingComma) {
			result.append(",");
		}
		return result.toString();
	}

	// Capitalize first char and any after underscore or space. Leave other caps
	// as-is.
	static String toTitleCase(String text) {
		final String[] parts = text.split("_ ");
		return Arrays.stream(parts).map(part -> part.substring(0, 1).toUpperCase() + part.substring(1))
				.collect(Collectors.joining());
	}

	static String firstCharToLowerCase(final String stringToConvert) {
		if (null != stringToConvert && !stringToConvert.isEmpty()) {
			char[] fieldNameChars = stringToConvert.toCharArray();
			fieldNameChars[0] = Character.toLowerCase(fieldNameChars[0]);
			return new String(fieldNameChars);
		} else {
			return stringToConvert;
		}
	}

	static String getFieldAvroType(String fieldName, String fixType, Map<String, Datatype> datatypes, String avroStandard) {
		Datatype datatype = datatypes.get(fixType);
		if (null == datatype) {
			throw new IllegalArgumentException(String.format("Orchestra datatype not found for received type: %s of Field: %s", fixType, fieldName));
		}
		MappedDatatype mappedDatatype = datatype.getMappedDatatype().stream().filter(md -> md.getStandard().equals(avroStandard))
	       .findFirst().orElse(null);
		StringBuffer avroType = new StringBuffer();
		if (null == mappedDatatype) {
			avroType.append("\"");
			switch (fixType) {
				case FLOAT_TYPE:
				case INT_TYPE:
					avroType.append(datatype.getName());
					break;
				case FIX_PRICE_TYPE:
				case "Amt":
				case "Qty":
				case "PriceOffset":
				case "NumInGroup":
				case "SeqNum":
				case "Length":
				case "TagNum":
				case "DayOfMonth":
				case "Percentage":
					avroType.append(datatype.getBaseType());
					break;
				case FIX_BOOLEAN_TYPE:
					avroType.append(SchemaGeneratorUtil.AVRO_BOOLEAN_TYPE);
					break;
				case FIX_CHAR_TYPE:
				case "data":
				case FIX_UTC_TIMESTAMP_TYPE:
				case "UTCTimeOnly":
				case "LocalMktTime":
				case "UTCDateOnly":
				case "LocalMktDate":
				case FIX_STRING_TYPE:
				default:
					avroType.append(SchemaGeneratorUtil.AVRO_STRING_TYPE);
			}
			avroType.append("\"");
		} else {
			Extension extension = mappedDatatype.getExtension();
			if (null == extension) {
				avroType.append(String.format("\"%s\"",mappedDatatype.getBase()));
			} else {
				avroType.append("{").append("\"type\": \"").append(mappedDatatype.getBase()).append("\", ");
				List<Object> any = extension.getAny();
				LogicalType logicalType = (LogicalType)any.get(0);
				avroType.append(String.format("\"logicalType\": \"%s\"", logicalType.getName()));
				List<KeyValue> keyValues = logicalType.getKeyValues();
				if (null != keyValues && keyValues.size() > 0) {
					avroType.append(", ");
					List<String> keyValueStrings = new ArrayList<>();
					keyValues.forEach(kv -> keyValueStrings.add(String.format("\"%s\": \"%s\"", kv.getKey(), kv.getValue())));
					avroType.append(String.join(", ",keyValueStrings));
				}
				avroType.append("}");
			}
		} 
		return avroType.toString();
	}

}
