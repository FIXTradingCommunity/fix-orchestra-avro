/*
 * Copyright 2017-2020 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package io.fixprotocol.orchestra.avro;

import static io.fixprotocol.orchestra.avro.SchemaGeneratorUtil.indent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import io.fixprotocol._2020.orchestra.repository.CategoryType;
import io.fixprotocol._2020.orchestra.repository.CodeSetType;
import io.fixprotocol._2020.orchestra.repository.CodeType;
import io.fixprotocol._2020.orchestra.repository.ComponentRefType;
import io.fixprotocol._2020.orchestra.repository.ComponentType;
import io.fixprotocol._2020.orchestra.repository.Documentation;
import io.fixprotocol._2020.orchestra.repository.FieldRefType;
import io.fixprotocol._2020.orchestra.repository.FieldType;
import io.fixprotocol._2020.orchestra.repository.GroupRefType;
import io.fixprotocol._2020.orchestra.repository.GroupType;
import io.fixprotocol._2020.orchestra.repository.MessageType;
import io.fixprotocol._2020.orchestra.repository.Repository;
import io.fixprotocol._2020.orchestra.repository.SectionType;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Generates Apache Avro schema from a FIX Orchestra file
 *
 */
public class SchemaGenerator {

	private static final String AVSC = ".avsc";

	static final int SPACES_PER_LEVEL = 2;
	
	private static final int COMPONENT_ID_STANDARD_TRAILER = 1025;
	private static final int COMPONENT_ID_STANDARD_HEADER = 1024;
	
	private static final String DOUBLE_TYPE = "double";
	private static final String STRING_TYPE = "string";
	
	private static final String CODE_SET_DIR = "codeset";
	private static final String COMPONENT_DIR = "component";
	private static final String MESSAGE_DIR = "message";
	private static final String GROUP_DIR = "group";

	protected static final String FIELD_DIR = "field";
	
	private boolean isGenerateStringForDecimal = true;
	private boolean isExcludeSession = false;
	private boolean isAppendRepoFixVersionToNamespace = true;
	private String namespace = null;
	
	/**
	 * Runs a SchemaGenerator with command line arguments
	 *
	 * @param args command line arguments. 
	 */
	public static void main(String[] args) {
		final SchemaGenerator generator = new SchemaGenerator();
		Options options = new Options();
		new CommandLine(options).execute(args);
		try (FileInputStream inputStream = new FileInputStream(new File(options.orchestraFileName))) {
			generator.setGenerateStringForDecimal(!options.isGenerateStringForDecimal);
			generator.setExcludeSession(options.isExcludeSession);
            generator.generate(inputStream, new File(options.outputDir));
            generator.setNamespace(options.namespace);
            generator.setAppendRepoFixVersionToNamespace(options.isAppendRepoFixVersionToNamespace);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	@Command(name = "Options", mixinStandardHelpOptions = true, description = "Options for generation of Apache Avro schema from a FIX Orchestra Repository")
	static class Options {
		static final String EXCLUDE_SESSION = "--excludeSession";

		@Option(names = { "-o", "--output-dir" }, defaultValue = "target/generated-sources", 
				paramLabel = "OUTPUT_DIRECTORY", description = "The output directory, Default : ${DEFAULT-VALUE}")
		String outputDir = "target/generated-sources";

		@Option(names = { "-i", "--orchestra-file" }, required = true, 
				paramLabel = "ORCHESTRA_FILE", description = "The path/name of the FIX OrchestraFile")
		String orchestraFileName;

		@Option(names = { "-n", "--namespace" }, required = true, 
				paramLabel = "NAMESPACE", description = "The namespace for the generated schema, include version if required.")
		String namespace;
		
		@Option(names = { "--generate-string-for-decimal" }, defaultValue = "false", fallbackValue = "true", 
				paramLabel = "GENERATE_STRING_FOR_DECIMAL", description = "Use String type for Decimal Fields instead of double, Default : ${DEFAULT-VALUE}")
		boolean isGenerateStringForDecimal = true;
		
		@Option(names = { "--append-repo-fix-version-to-namespace" }, defaultValue = "true", fallbackValue = "true", 
				paramLabel = "APPEND_REPO_FIX_VERSION_TO_NAMESPACE", description = "Append the FIX version specified in the repository to the namespace, Default : ${DEFAULT-VALUE}")
		boolean isAppendRepoFixVersionToNamespace = true;
		
		@Option(names = { EXCLUDE_SESSION }, defaultValue = "false", fallbackValue = "true", 
				paramLabel = "EXCLUDE_SESSION", description ="Excludes Session Category Messages, Components and Groups exclusive to Session Layer and Fields used by Session Layer from the generated code, Default : ${DEFAULT-VALUE}")
		boolean isExcludeSession = false;
	}

	private String decimalTypeString = STRING_TYPE;

	private Repository repository;
	
	void initialise(InputStream inputFile) throws JAXBException {
		this.repository = unmarshal(inputFile);
		if (isAppendRepoFixVersionToNamespace) {
			String version = repository.getVersion();
			// Split off EP portion of version
			final String[] parts = version.split("_");
			if (parts.length > 0) {
				version = parts[0];
			}
			final String versionStr = version.replaceAll("[\\.]", "").toLowerCase();
			this.namespace = this.namespace.concat(".").concat(versionStr);
		}
	}
	
	public void generate(InputStream inputFile, File outputDir) {
		try {
			if (!this.isGenerateStringForDecimal) {
				decimalTypeString = DOUBLE_TYPE;
			}
			
			Map<String, SectionType> sections = new HashMap<String, SectionType>();
			Map<String, CategoryType> categories = new HashMap<String, CategoryType>();
			Map<String, CodeSetType> codeSets = new HashMap<>();
			Map<Integer, ComponentType> components = new HashMap<>();
			Map<Integer, FieldType> fields = new HashMap<>();
			Map<Integer, GroupType> groups = new HashMap<>();
			Set<BigInteger> sessionFieldIds = new HashSet<BigInteger>();
			Set<BigInteger> nonSessionFieldIds = new HashSet<BigInteger>();
			
			initialise(inputFile);

			repository.getSections().getSection().forEach(s -> {sections.put(s.getName(), s);});
			repository.getCategories().getCategory().forEach(c -> {categories.put(c.getName(), c);});
			
			final List<MessageType> messages = repository.getMessages().getMessage();
			final List<FieldType> fieldList = this.repository.getFields().getField();
			final List<ComponentType> componentList = repository.getComponents().getComponent();
			for (final FieldType fieldType : fieldList) {
				BigInteger id = fieldType.getId();
				fields.put(id.intValue(), fieldType);
			}
			for (final ComponentType component : componentList) {
				components.put(component.getId().intValue(), component);
			}
			List<GroupType> groupList = repository.getGroups().getGroup();
			for (final GroupType group : groupList) {
				groups.put(group.getId().intValue(), group);
			}
			// create new maps of groups so that we can keep original groups map unaltered 
			Map<Integer, GroupType> nonSessionGroups = new HashMap<Integer, GroupType>();
			Map<Integer, GroupType> sessionGroups = new HashMap<Integer, GroupType>();
			// derive set of session messages
			List<MessageType> sessionMessages = excludeSessionMessages(messages);
			//collect groups and field ids from the non-session messages
			collectGroupsAndFields(messages, nonSessionFieldIds, groups, nonSessionGroups, components, fields, false);
			//collect groups and field ids from the session messages
			collectGroupsAndFields(sessionMessages, sessionFieldIds, groups, sessionGroups, components, fields, true);
			//restrict nonSessionFieldIds, removing fields that are used in session messages 
			//if option is selected for separate package of session messages and fields, the fields will be provided by the session package and not duplicated
			nonSessionFieldIds.removeAll(sessionFieldIds);
			
			repository.getCodeSets().getCodeSet().forEach(codeSet -> {codeSets.put(codeSet.getName(), codeSet);});
			
			final File typeDir = SchemaGeneratorUtil.getAvroPath(outputDir, namespace, CODE_SET_DIR);
			typeDir.mkdirs();
			final File fieldDir = SchemaGeneratorUtil.getAvroPath(outputDir, namespace, FIELD_DIR);
			fieldDir.mkdirs();
			final File componentDir = SchemaGeneratorUtil.getAvroPath(outputDir, namespace, COMPONENT_DIR);
			componentDir.mkdirs();
			final File groupDir = SchemaGeneratorUtil.getAvroPath(outputDir, namespace, GROUP_DIR);
			groupDir.mkdirs();
			final File messageDir = SchemaGeneratorUtil.getAvroPath(outputDir, namespace, MESSAGE_DIR);
			messageDir.mkdirs();
			
			for (final CodeSetType codeSet : codeSets.values()) {
				final String name = codeSet.getName();
				final String fixType = codeSet.getType();
				final File codeSetFile = getFilePath(typeDir, name);
				generateCodeSet(namespace, decimalTypeString, name, codeSet, fixType, codeSetFile);
			}
			
			for (final FieldType fieldType : fieldList) {
				BigInteger id = fieldType.getId();				
				if (isExcludeSession) {
					if (nonSessionFieldIds.contains(id)) {
						generateField(fieldType, this.namespace, typeDir, fieldDir, codeSets, this.decimalTypeString);
					}
				} else {
					generateField(fieldType, this.namespace, typeDir, fieldDir, codeSets, this.decimalTypeString);
				}
			}

			for (final GroupType group : nonSessionGroups.values()) {
				generateGroup(groupDir, group, this.namespace, nonSessionGroups, components, fields, codeSets);
			}
			if (!isExcludeSession) { // process groups that are used by session messages
				for (final GroupType group : sessionGroups.values()) {
					generateGroup(groupDir, group, this.namespace, sessionGroups, components, fields, codeSets);
				}
			}
//			// at time of writing Session Messages do not contain components (only groups),
//			// so there is no logic to segregate session components
			for (final ComponentType component : componentList) {
				int id = component.getId().intValue();
				if (id != COMPONENT_ID_STANDARD_HEADER && id != COMPONENT_ID_STANDARD_TRAILER) {
					generateComponent(componentDir, component, this.namespace, this.decimalTypeString, groups, components, fields, codeSets);
				}
			}
			generateMessages(messageDir, messages, namespace, this.decimalTypeString, groups, components, fields, codeSets);
			if (!isExcludeSession) {
				generateMessages(messageDir, sessionMessages, namespace, this.decimalTypeString, groups, components, fields, codeSets);
			}
		} catch (JAXBException | IOException e) {
			e.printStackTrace();
		}
	}



	/**
	 * Excludes Session messages
	 * @param messageList 
	 * @return the MessageTypes that have been excluded
	 */
	private static List<MessageType> excludeSessionMessages(List<MessageType> messageList) {
		List<MessageType> excludedMessages = new ArrayList<MessageType>(); 
		for (final MessageType message : messageList) {
			if (message.getCategory().equals("Session")) {
				excludedMessages.add(message);
			}
		}
		messageList.removeAll(excludedMessages);
		return excludedMessages;
	}

	private static void generateMessages(File outputDir, 
			                             final List<MessageType> messages, 
			                             final String namespace,
			                             final String decimalTypeString, 
			                             Map<Integer, GroupType> groups, 
			                             Map<Integer, ComponentType> components, 
			                             Map<Integer, FieldType> fields, Map<String, CodeSetType> codeSets) throws IOException {
		for (final MessageType message : messages) {
			generateMessage(outputDir, message, namespace, decimalTypeString, groups,  components, fields, codeSets);
		}
	}
	
	private static void generateMessage(File messageDir, MessageType message, String namespace,
			String decimalTypeString, Map<Integer, GroupType> groups, Map<Integer, ComponentType> components,
			Map<Integer, FieldType> fields, Map<String, CodeSetType> codeSets) throws IOException {
		final String name = SchemaGeneratorUtil.toTitleCase(message.getName());
		
		final File messageFile = getFilePath(messageDir, name);
		try (FileWriter writer = new FileWriter(messageFile)) {
			SchemaGeneratorUtil.writeOpenBracket(writer);
			SchemaGeneratorUtil.writeName(writer, name, 1);
			SchemaGeneratorUtil.writeNameSpace(writer, namespace, MESSAGE_DIR);
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "type", "record");
			writer.write("\n");

			List<Object> docMembers = message.getAnnotation().getDocumentationOrAppinfo();
			List<String> docs = new ArrayList<>();
			for (Object member : docMembers) {
				if (member instanceof Documentation) {
					((Documentation) member).getContent().forEach(d -> {
						docs.add(d.toString().trim());
					});
				}
			}
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "doc", String.join(",", docs).trim());	
			writer.write("\n");
			
			writer.write(indent(1));
			writer.write("\"fields\": [\n");

			final List<Object> members = message.getStructure().getComponentRefOrGroupRefOrFieldRef();
			writeMembersInline(writer, members, namespace, decimalTypeString, groups, components, fields, codeSets, 2);
			
			writer.write(SchemaGeneratorUtil.indent(1));
			writer.write("]\n");
			SchemaGeneratorUtil.writeCloseBracket(writer);
		}
	}

	private static void collectGroupsAndFields(List<MessageType> messageList, Set<BigInteger> includedFieldIds, 
			                    Map<Integer, GroupType> groups, 
			                    Map<Integer, GroupType> collectedGroups, 
			                    Map<Integer, ComponentType> components, 
			                    Map<Integer, FieldType> fields,
                                boolean isParseHeaderAndTrailer) throws IOException {
		for (final MessageType messageType : messageList) {
			final List<Object> members = messageType.getStructure().getComponentRefOrGroupRefOrFieldRef();
			collectGroupsAndFieldsFromMembers(members, includedFieldIds, groups, collectedGroups, components, fields, isParseHeaderAndTrailer);
		}
	}

	private static void collectGroupsAndFieldsFromMembers(List<Object> members, Set<BigInteger> includedFieldIds, 
			                                      Map<Integer, GroupType> groups, 
			                                      Map<Integer, GroupType> collectedGroups, 
			                                      Map<Integer, ComponentType> components, 
			                                      Map<Integer, FieldType> fields,
			                                      boolean isParseHeaderAndTrailer) throws IOException {
		for (final Object member : members) {
			if (member instanceof FieldRefType) {
				final FieldRefType fieldRefType = (FieldRefType) member;
				includedFieldIds.add(fieldRefType.getId());
			} else if (member instanceof GroupRefType) {
				final int id = ((GroupRefType) member).getId().intValue();
				final GroupType groupType = groups.get(id);
				if (groupType != null) {
					collectedGroups.put(groupType.getId().intValue(), groupType);
					final int numInGroupId = groupType.getNumInGroup().getId().intValue();
					final FieldType numInGroupField = fields.get(numInGroupId);
					includedFieldIds.add(numInGroupField.getId());
					collectGroupsAndFieldsFromMembers(groupType.getComponentRefOrGroupRefOrFieldRef(),includedFieldIds, groups, collectedGroups, components, fields, isParseHeaderAndTrailer);
				} else {
					System.err.format("collectFieldIdsFromMembers : Group missing from repository; id=%d%n", id);
				}
			} else if (member instanceof ComponentRefType) {
				final int id = ((ComponentRefType) member).getId().intValue();
				final ComponentType componentType = components.get(id);
				if (null != componentType) {
					if ((id != COMPONENT_ID_STANDARD_HEADER && id != COMPONENT_ID_STANDARD_TRAILER) || isParseHeaderAndTrailer) {
						collectGroupsAndFieldsFromMembers(componentType.getComponentRefOrGroupRefOrFieldRef(),includedFieldIds, groups, collectedGroups, components, fields, isParseHeaderAndTrailer);
					}
				} else {
					System.err.format("Component missing from repository; id=%d%n", id);
				}
			}
		}
	}

	private static void generateComponent(File componentDir,
			              				  ComponentType componentType,
			              				  String namespace,
			              				  String decimalTypeString,
			              				  Map<Integer, GroupType> groups, 
			              				  Map<Integer, ComponentType> components, 
			              				  Map<Integer, FieldType> fields,
										  Map<String, CodeSetType> codeSets) throws IOException {
		final String name = SchemaGeneratorUtil.toTitleCase(componentType.getName());
		
		final File componentFile = getFilePath(componentDir, name);
		try (FileWriter writer = new FileWriter(componentFile)) {
			SchemaGeneratorUtil.writeOpenBracket(writer);
			SchemaGeneratorUtil.writeName(writer, name, 1);
			SchemaGeneratorUtil.writeNameSpace(writer, namespace, COMPONENT_DIR);
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "type", "record");
			writer.write("\n");

			List<Object> docMembers = componentType.getAnnotation().getDocumentationOrAppinfo();
			List<String> docs = new ArrayList<>();
			for (Object member : docMembers) {
				if (member instanceof Documentation) {
					((Documentation) member).getContent().forEach(d -> {
						docs.add(d.toString().trim());
					});
				}
			}
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "doc", String.join(",", docs).trim());	
			writer.write("\n");
			
			writer.write(indent(1));
			writer.write("\"fields\": [\n");

			final List<Object> members = componentType.getComponentRefOrGroupRefOrFieldRef();
			writeMembersInline(writer, members, namespace, decimalTypeString, groups, components, fields, codeSets, 2);
			
			writer.write(SchemaGeneratorUtil.indent(1));
			writer.write("]\n");
			SchemaGeneratorUtil.writeCloseBracket(writer);
		}
	}

	private static void writeMembersInline(FileWriter writer, 
									  List<Object> members, 
									  String namespace,
									  String decimalTypeString,
									  Map<Integer, GroupType> groups, 
									  Map<Integer, ComponentType> components, 
									  Map<Integer, FieldType> fields,
									  Map<String, CodeSetType> codeSets,
									  int indent) throws IOException {
		List<String> memberStrings = new ArrayList<>();
		for (final Object member : members) {
			if (member instanceof FieldRefType) {
				final FieldRefType fieldRefType = (FieldRefType) member;
				int id = fieldRefType.getId().intValue();
				final FieldType fieldType = fields.get(id);
				if (fieldType != null) {
					final String fieldTypeType = SchemaGeneratorUtil.toTitleCase(fieldType.getType());
					final CodeSetType codeSet = codeSets.get(fieldTypeType);
					if (null == codeSet) {
						final String avroType = getFieldAvroType(fieldTypeType, decimalTypeString);
						memberStrings.add(SchemaGeneratorUtil.getFieldInlineString(fieldRefType, fieldType, fieldTypeType, avroType, indent));
					} else {
						final String generatedType = namespace.concat(".").concat(CODE_SET_DIR).concat(".").concat(codeSet.getName());
						memberStrings.add(SchemaGeneratorUtil.getFieldInlineString(fieldRefType, fieldType, fieldTypeType, generatedType, indent));
					}
				} else {
					System.err.format("writeMembersInline : Field missing from repository; id=%d%n", id);
				}
			} else if (member instanceof GroupRefType) {
				GroupRefType groupRefType = (GroupRefType) member;
				final int id = groupRefType.getId().intValue();
				final GroupType groupType = groups.get(id);
				if (groupType != null) {
					final String groupTypeName = SchemaGeneratorUtil.toTitleCase(groupType.getName());
					final String generatedType = namespace.concat(".").concat(GROUP_DIR).concat(".").concat(groupTypeName);
					memberStrings.add(getGroupInlineString(groupRefType, groupType, groupTypeName, generatedType));
				} else {
					System.err.format("writeMembersInline : Group missing from repository; id=%d%n", id);
				}
			} else if (member instanceof ComponentRefType) {
				final ComponentRefType componentRefType = (ComponentRefType) member;
				final int id = componentRefType.getId().intValue();
				final ComponentType componentType = components.get(id);
				if (componentType != null) {
					final String componentTypeName = SchemaGeneratorUtil.toTitleCase(componentType.getName());
					final String generatedType = namespace.concat(".").concat(COMPONENT_DIR).concat(".").concat(componentTypeName);
					memberStrings.add(getComponentInlineString(componentRefType, componentType, componentTypeName, generatedType));
				} else {
					System.err.format("writeMembersInline : Component missing from repository; id=%d%n", id);
				}
			}
		}
		writer.write(String.join(",\n", memberStrings));
		writer.write("\n");
	}

	private static String getComponentInlineString(ComponentRefType componentRefType, ComponentType componentType, String name, String type) {
		StringBuffer result = new StringBuffer();
		result.append(indent(2));
		result.append("{");
		result.append(SchemaGeneratorUtil.getJsonNameValue("name", name, true));
		result.append(SchemaGeneratorUtil.getJsonNameValue("type", type, true));

		List<Object> members = componentRefType.getAnnotation().getDocumentationOrAppinfo();
		List<String> docs = new ArrayList<>();
		SchemaGeneratorUtil.getDocumentationStrings(members, docs);
		docs.add("Component : ".concat(componentType.getName()));
		members = componentType.getAnnotation().getDocumentationOrAppinfo();
		SchemaGeneratorUtil.getDocumentationStrings(members, docs);
		
		result.append(SchemaGeneratorUtil.getJsonNameValue("doc", String.join(",", docs).trim(), false));			
		result.append("}");
		return result.toString();
	}

	private static String getGroupInlineString(GroupRefType groupRefType, GroupType groupType, String name,	String type) {
		StringBuffer result = new StringBuffer();
		result.append(indent(2));
		result.append("{");
		result.append(SchemaGeneratorUtil.getJsonNameValue("name", name, true));
		result.append(SchemaGeneratorUtil.getJsonNameValue("type", type, true));

		List<Object> members = groupRefType.getAnnotation().getDocumentationOrAppinfo();
		List<String> docs = new ArrayList<>();
		SchemaGeneratorUtil.getDocumentationStrings(members, docs);
		docs.add("Group : ".concat(groupType.getName()));
		members = groupType.getAnnotation().getDocumentationOrAppinfo();
		SchemaGeneratorUtil.getDocumentationStrings(members, docs);
		
		result.append(SchemaGeneratorUtil.getJsonNameValue("doc", String.join(",", docs).trim(), false));			
		result.append("}");
		return result.toString();
	}

	private static void generateField(FieldType fieldType, String namespace, File typeDir, File fieldDir,
			Map<String, CodeSetType> codeSets, String decimalTypeString)
			throws IOException {
		final String name = SchemaGeneratorUtil.toTitleCase(fieldType.getName());
		String fieldTypeName = fieldType.getType();
		final CodeSetType codeSet = codeSets.get(fieldTypeName);
		final File fieldFile = getFilePath(fieldDir, name);
		if (null == codeSet) {
			final String avroType = getFieldAvroType(fieldTypeName, decimalTypeString);
			generateField(fieldType, namespace, decimalTypeString, name, avroType, fieldFile);
		} else {
			final String generatedType = namespace.concat(".").concat(CODE_SET_DIR).concat(".").concat(fieldTypeName);
			generateField(fieldType, namespace, decimalTypeString, name, generatedType, fieldFile);
		}
	}

	private static void generateCodeSet(String namespace, String decimalTypeString, final String name,
			final CodeSetType codeSet, final String fixType, final File typeFile) throws IOException {
		try (FileWriter writer = new FileWriter(typeFile)) {
			SchemaGeneratorUtil.writeOpenBracket(writer);
			SchemaGeneratorUtil.writeName(writer, name, 1);
			SchemaGeneratorUtil.writeNameSpace(writer, namespace, CODE_SET_DIR);
			SchemaGeneratorUtil.writeEnumDef(writer);
//			final String avroType = getFieldAvroType(fixType, decimalTypeString);
			List<CodeType> codes = codeSet.getCode();
			String unknown = "UNKNOWN_".concat(SchemaGeneratorUtil.precedeCapsWithUnderscore(codeSet.getName()));
			for (CodeType code : codes) {
				writer.write(indent(3));
				writer.write("\"");
				writer.write(code.getName());
				writer.write("\",\n");
			}
			writer.write(indent(3));
			writer.write("\"");
			writer.write(unknown);
			writer.write("\"\n");
			SchemaGeneratorUtil.writeEndEnumDef(writer, name, unknown);
			SchemaGeneratorUtil.writeCloseBracket(writer);
		}
	}

	private static void generateField(FieldType fieldType, String namespace, String decimalTypeString,
			final String name, final String type, final File file) throws IOException {
		try (FileWriter writer = new FileWriter(file)) {
			SchemaGeneratorUtil.writeField(fieldType, namespace, name, type, writer);
		}
	}

	private void generateGroup(File groupDir, GroupType group, String namespace,
			Map<Integer, GroupType> groups, Map<Integer, ComponentType> components,
			Map<Integer, FieldType> fields, Map<String, CodeSetType> codeSets) throws IOException {
		final String name = SchemaGeneratorUtil.toTitleCase(group.getName());
		final File file = getFilePath(groupDir, name);
		try (FileWriter writer = new FileWriter(file)) {
			SchemaGeneratorUtil.writeOpenBracket(writer);
			SchemaGeneratorUtil.writeName(writer, name, 1);
			SchemaGeneratorUtil.writeNameSpace(writer, namespace, GROUP_DIR);
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "type", "record");
			writer.write("\n");

			List<Object> docMembers = group.getAnnotation().getDocumentationOrAppinfo();
			List<String> docs = new ArrayList<>();
			for (Object member : docMembers) {
				if (member instanceof Documentation) {
					((Documentation) member).getContent().forEach(d -> {
						docs.add(d.toString().trim());
					});
				}
			}
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "doc", String.join(",", docs).trim());	
			writer.write("\n");
			
			writer.write(indent(1));
			writer.write("\"fields\": [\n");
			
			writer.write(indent(2));
			writer.write("{\n");
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(3), "name", name, true);
			writer.write("\n");
			writer.write(indent(3));
			writer.write("\"type\": ");
			writer.write("{\n");
			
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(4), "type", "array");
			writer.write("\n");
			writer.write(indent(4));
			writer.write("\"items\": ");
			writer.write("{\n");
			
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(5), "name", name.concat("Entry"), true);
			writer.write("\n");
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(5), "type", "record");
			writer.write("\n");
			writer.write(indent(5));
			writer.write("\"fields\": [\n");
			final List<Object> members = group.getComponentRefOrGroupRefOrFieldRef();
			writeMembersInline(writer, members, namespace, decimalTypeString, groups, components, fields, codeSets, 6);
			writer.write(indent(5));
			writer.write("]\n");

			writer.write(indent(4));
			writer.write("}\n");
			writer.write(indent(3));
			writer.write("}\n");
			writer.write(indent(2));
			writer.write("}\n");
			writer.write(indent(1));
			writer.write("]\n"); 
			SchemaGeneratorUtil.writeCloseBracket(writer);
		}
	}	
	
	private static File getFilePath(File dir, String typeName) {
		final StringBuilder sb = new StringBuilder();
		sb.append(File.separatorChar);
		sb.append(typeName);
		sb.append(AVSC);
		return new File(dir, sb.toString());
	}

	private static String getFieldAvroType(String type, String decimalTypeString) {
		String avroType;
		switch (type) {
		case "Price":
		case "Amt":
		case "Qty":
		case "float":
		case "PriceOffset":
			avroType = decimalTypeString;
			break;
		case "int":
		case "NumInGroup":
		case "SeqNum":
		case "Length":
		case "TagNum":
		case "DayOfMonth":
			avroType = "int";
			break;
		case "Boolean":
			avroType = "boolean ";
			break;
		case "Percentage":
			avroType = DOUBLE_TYPE;
			break;
		case "char":
		case "UTCTimestamp":
		case "UTCTimeOnly":
		case "LocalMktTime":
		case "UTCDateOnly":
		case "LocalMktDate":
		default:
			avroType = "string";
		}
		return avroType;
	}

	private static Repository unmarshal(InputStream inputFile) throws JAXBException {
		final JAXBContext jaxbContext = JAXBContext.newInstance(Repository.class);
		final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		return (Repository) jaxbUnmarshaller.unmarshal(inputFile);
	}

	public void setGenerateStringForDecimal(boolean isGenerateBigDecimal) {
		this.isGenerateStringForDecimal = isGenerateBigDecimal;
	}

	public boolean isGenerateStringForDecimal() {
		return this.isGenerateStringForDecimal;
	}

	public boolean isExcludeSession() {
		return isExcludeSession;
	}

	public void setExcludeSession(boolean isExcludeSession) {
		this.isExcludeSession = isExcludeSession;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public boolean isAppendRepoFixVersionToNamespace() {
		return isAppendRepoFixVersionToNamespace;
	}

	public void setAppendRepoFixVersionToNamespace(boolean isAppendRepoFixVersionToNamespace) {
		this.isAppendRepoFixVersionToNamespace = isAppendRepoFixVersionToNamespace;
	}
}
