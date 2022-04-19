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

import static io.fixprotocol.orchestra.avro.SchemaGeneratorUtil.getJsonNameValue;
import static io.fixprotocol.orchestra.avro.SchemaGeneratorUtil.indent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import io.fixprotocol._2020.orchestra.repository.CategoryType;
import io.fixprotocol._2020.orchestra.repository.CodeSetType;
import io.fixprotocol._2020.orchestra.repository.CodeType;
import io.fixprotocol._2020.orchestra.repository.ComponentRefType;
import io.fixprotocol._2020.orchestra.repository.ComponentType;
import io.fixprotocol._2020.orchestra.repository.Datatype;
import io.fixprotocol._2020.orchestra.repository.FieldRefType;
import io.fixprotocol._2020.orchestra.repository.FieldType;
import io.fixprotocol._2020.orchestra.repository.GroupRefType;
import io.fixprotocol._2020.orchestra.repository.GroupType;
import io.fixprotocol._2020.orchestra.repository.MessageType;
import io.fixprotocol._2020.orchestra.repository.PresenceT;
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

	public  static final String AVRO_V1 = "AvroV1";

	private static final String AVSC = ".avsc";

	static final int SPACES_PER_LEVEL = 2;
	
	private static final String CODE_SET_DIR = "codesets";
	private static final String COMPONENT_DIR = "components";
	private static final String MESSAGE_DIR = "messages";
	private static final String GROUP_DIR = "groups";

	protected static final String FIELD_DIR = "fields";

	private boolean isNormaliseComponents = false;
	private boolean isNormaliseGroups = false;

	private boolean isAppendRepoFixVersionToNamespace = true;
	private String namespace = null;
	private Repository repository;
	private String avroStandard = AVRO_V1;

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
            generator.generate(inputStream, new File(options.outputDir));
            generator.setNamespace(options.namespace);
            generator.setNormaliseComponents(options.isNormaliseComponents);
            generator.setNormaliseGroups(options.isNormaliseGroups);
            generator.setAvroStandard(options.avroStandard);
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
				paramLabel = "ORCHESTRA_FILE", description = "The path/name of the FIX Orchestra file")
		String orchestraFileName;

		@Option(names = { "-n", "--namespace" }, required = true, 
				paramLabel = "NAMESPACE", description = "The namespace for the generated schema, include version if required.")
		String namespace;
		
		@Option(names = { "-a", "--avro-standard" }, required = false, defaultValue = AVRO_V1, 
				paramLabel = "AVRO-STANDARD", description = "The value expected in the \"standard\" attribute of the mappedDatatype elements of the Orchestra File, Default : ${DEFAULT-VALUE}")
		String avroStandard  = AVRO_V1;
		
		@Option(names = { "--normalise-groups" }, defaultValue = "false", fallbackValue = "true",
				paramLabel = "NORMALISE-GROUPS", description = "Normalise Groups by writing the schemas to separate files.")
		boolean isNormaliseGroups = false;

		@Option(names = { "--normalise-components" }, defaultValue = "false", fallbackValue = "true",
				paramLabel = "NORMALISE-COMPONENTS", description = "Normalise Componenents by writing the schemas to separate files.")
		boolean isNormaliseComponents = false;
		
		@Option(names = { "--append-repo-fix-version-to-namespace" }, defaultValue = "true", fallbackValue = "true", 
				paramLabel = "APPEND_REPO_FIX_VERSION_TO_NAMESPACE", description = "Append the FIX version specified in the Orchestra repository file to the namespace, Default : ${DEFAULT-VALUE}")
		boolean isAppendRepoFixVersionToNamespace = true;
	}
	
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
			Map<String, SectionType> sections = new HashMap<String, SectionType>();
			Map<String, CategoryType> categories = new HashMap<String, CategoryType>();
			Map<String, CodeSetType> codeSets = new HashMap<>();
			Map<Integer, ComponentType> components = new HashMap<>();
			Map<Integer, FieldType> fields = new HashMap<>();
			Map<Integer, GroupType> groups = new HashMap<>();
			Map<String, Datatype> datatypes = new HashMap<>();
			
			initialise(inputFile);

			repository.getSections().getSection().forEach(s -> {sections.put(s.getName(), s);});
			repository.getCategories().getCategory().forEach(c -> {categories.put(c.getName(), c);});
			repository.getDatatypes().getDatatype().forEach(d -> {datatypes.put(d.getName(), d);});
			
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
			repository.getCodeSets().getCodeSet().forEach(codeSet -> {codeSets.put(codeSet.getName(), codeSet);});
			
			final File typeDir = SchemaGeneratorUtil.getAvroPath(outputDir, namespace, CODE_SET_DIR);
			typeDir.mkdirs();
			final File messageDir = SchemaGeneratorUtil.getAvroPath(outputDir, namespace, MESSAGE_DIR);
			messageDir.mkdirs();
			
			for (final CodeSetType codeSet : codeSets.values()) {
				final String name = codeSet.getName();
				final String fixType = codeSet.getType();
				final File codeSetFile = getFilePath(typeDir, name);
				generateCodeSet(namespace, name, codeSet, fixType, codeSetFile);
			}
			if (isNormaliseGroups) {
				final File groupDir = SchemaGeneratorUtil.getAvroPath(outputDir, namespace, GROUP_DIR);
				groupDir.mkdirs();
				for (final GroupType group : groups.values()) {
					generateGroup(groupDir, group, this.namespace, groups, components, fields, codeSets, datatypes);
				}
			}
			// at time of writing Session Messages do not contain components (only groups),
			// so there is no logic to segregate session components
			if (isNormaliseComponents) {
				final File componentDir = SchemaGeneratorUtil.getAvroPath(outputDir, namespace, COMPONENT_DIR);
				componentDir.mkdirs();
				for (final ComponentType component : componentList) {
					generateComponent(componentDir, component, this.namespace, groups, components, fields, codeSets, datatypes, this.isNormaliseComponents, this.isNormaliseGroups, this.avroStandard);
				}
			}
			generateMessages(messageDir, messages, namespace, groups, components, fields, codeSets, datatypes, this.isNormaliseComponents, this.isNormaliseGroups, this.avroStandard);
		} catch (JAXBException | IOException e) {
			e.printStackTrace();
		}
	}


	private static void generateMessages(File outputDir, 
			                             final List<MessageType> messages, 
			                             final String namespace,
			                             Map<Integer, GroupType> groups, 
			                             Map<Integer, ComponentType> components, 
			                             Map<Integer, FieldType> fields, 
			                             Map<String, CodeSetType> codeSets,
			                             Map<String, Datatype> datatypes,
										 boolean isNormaliseComponents, 
										 boolean isNormaliseGroups,
										 String avroStandard) throws IOException {
		for (final MessageType message : messages) {
			generateMessage(outputDir, message, namespace, groups,  components, fields, codeSets, datatypes, isNormaliseComponents, isNormaliseGroups, avroStandard);
		}
	}
	
	private static void generateMessage(File messageDir, MessageType message, String namespace,
			Map<Integer, GroupType> groups, Map<Integer, ComponentType> components,
			Map<Integer, FieldType> fields, Map<String, CodeSetType> codeSets, Map<String, Datatype> datatypes, boolean isNormaliseComponents, boolean isNormaliseGroups, String avroStandard) throws IOException {
		final String name = SchemaGeneratorUtil.toTitleCase(message.getName());
		
		final File messageFile = getFilePath(messageDir, name);
		
		try (Writer writer = new OutputStreamWriter(new FileOutputStream(messageFile), StandardCharsets.UTF_8)) {
			SchemaGeneratorUtil.writeOpenBracket(writer);
			SchemaGeneratorUtil.writeName(writer, name, 1);
			SchemaGeneratorUtil.writeNameSpace(writer, namespace, MESSAGE_DIR);
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "type", "record");
			writer.write("\n");

			List<Object> docMembers = message.getAnnotation().getDocumentationOrAppinfo();
			List<String> docs = new ArrayList<>();
			SchemaGeneratorUtil.getDocumentationStrings(docMembers, docs);

			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "doc", String.join(",", docs).trim());	
			writer.write("\n");
			
			writer.write(indent(1));
			writer.write("\"fields\": [\n");

			final List<Object> members = message.getStructure().getComponentRefOrGroupRefOrFieldRef();
			List<String> memberStrings = new ArrayList<>();
			getMembersInline(members, namespace, groups, components, fields, codeSets, datatypes, 1, memberStrings, isNormaliseComponents, isNormaliseGroups, avroStandard);
			writer.write(String.join(",\n", memberStrings));
			writer.write("\n");
			writer.write(SchemaGeneratorUtil.indent(1));
			writer.write("]\n");
			SchemaGeneratorUtil.writeCloseBracket(writer);
		}
	}

	private static void generateComponent(File componentDir,
			              				  ComponentType componentType,
			              				  String namespace,
			              				  Map<Integer, GroupType> groups, 
			              				  Map<Integer, ComponentType> components, 
			              				  Map<Integer, FieldType> fields,
										  Map<String, CodeSetType> codeSets,
										  Map<String, Datatype> datatypes,
										  boolean isNormaliseComponents,
										  boolean isNormaliseGroups,
										  String avroStandard) throws IOException {
		final String name = SchemaGeneratorUtil.toTitleCase(componentType.getName());
		
		final File componentFile = getFilePath(componentDir, name);
		try (Writer writer = new OutputStreamWriter(new FileOutputStream(componentFile), StandardCharsets.UTF_8)) {
			SchemaGeneratorUtil.writeOpenBracket(writer);
			SchemaGeneratorUtil.writeName(writer, name, 1);
			SchemaGeneratorUtil.writeNameSpace(writer, namespace, COMPONENT_DIR);
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "type", "record");
			writer.write("\n");

			List<Object> docMembers = componentType.getAnnotation().getDocumentationOrAppinfo();
			List<String> docs = new ArrayList<>();
			SchemaGeneratorUtil.getDocumentationStrings(docMembers, docs);

			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "doc", String.join(",", docs).trim());	
			writer.write("\n");
			
			writer.write(indent(1));
			writer.write("\"fields\": [\n");

			final List<Object> members = componentType.getComponentRefOrGroupRefOrFieldRef();
			List<String> memberStrings = new ArrayList<>();
			getMembersInline(members, namespace, groups, components, fields, codeSets, datatypes, 1, memberStrings, isNormaliseComponents, isNormaliseGroups, avroStandard);
			writer.write(String.join(",\n", memberStrings));
			writer.write("\n");
			writer.write(SchemaGeneratorUtil.indent(1));
			writer.write("]\n");
			SchemaGeneratorUtil.writeCloseBracket(writer);
		}
	}
	
	private static void getMembersInline(List<Object> members, 
									  String namespace,
									  Map<Integer, GroupType> groups, 
									  Map<Integer, ComponentType> components, 
									  Map<Integer, FieldType> fields,
									  Map<String, CodeSetType> codeSets,
									  Map<String, Datatype> datatypes,
									  int indent,
									  List<String> memberStrings,
									  boolean isNormaliseComponents, 
									  boolean isNormaliseGroups,
									  String avroStandard) throws IOException {
		for (final Object member : members) {
			if (member instanceof FieldRefType) {
				final FieldRefType fieldRefType = (FieldRefType) member;
				int id = fieldRefType.getId().intValue();
				final FieldType fieldType = fields.get(id);
				if (fieldType != null) {
					int fieldIndent = indent + 1;
					final String fixType = fieldType.getType();
					final CodeSetType codeSet = codeSets.get(fixType);
					String fieldName = SchemaGeneratorUtil.firstCharToLowerCase(fieldType.getName());
					if (null == codeSet) {
						final String avroType = SchemaGeneratorUtil.getFieldAvroType(fieldName, fixType, datatypes, avroStandard);
						memberStrings.add(SchemaGeneratorUtil.getFieldInlineString(fieldRefType, fieldType, fieldName, avroType, fieldIndent));
					} else {
						final String generatedType = namespace.concat(".").concat(CODE_SET_DIR).concat(".").concat(codeSet.getName());
						memberStrings.add(SchemaGeneratorUtil.getFieldInlineString(fieldRefType, fieldType, fieldName, "\"".concat(generatedType).concat("\""), fieldIndent));
					}
				} else {
					System.err.format("writeMembersInline : Field missing from repository; id=%d%n", id);
				}
			} else if (member instanceof GroupRefType) {
				GroupRefType groupRefType = (GroupRefType) member;
				final int id = groupRefType.getId().intValue();
				final GroupType groupType = groups.get(id);
				if (groupType != null) {
					int groupIndent = indent + 1;
					String groupTypeName = groupType.getName();
					if (isNormaliseGroups) {
						final String memberName = SchemaGeneratorUtil.firstCharToLowerCase(groupTypeName);
						final String generatedType = namespace.concat(".").concat(GROUP_DIR).concat(".").concat(groupTypeName);
						memberStrings.add(getGroupInlineString(groupRefType, groupType, memberName, generatedType));
					} else {
						StringBuffer groupString = new StringBuffer();
						groupString.append(indent(groupIndent));
						groupString.append("{\n");
						groupString.append(indent(++groupIndent)).append(getJsonNameValue("name", SchemaGeneratorUtil.firstCharToLowerCase(groupTypeName), true));
						groupString.append("\n");
						groupString.append(indent(groupIndent));
						if (!groupRefType.getPresence().equals(PresenceT.REQUIRED)) {
							groupString.append("\"type\": [\n");
							++groupIndent;
							groupString.append(indent(groupIndent)).append("\"null\", \n");
							groupString.append(indent(groupIndent)).append("{\n");
							++groupIndent;
						} else {
							groupString.append("\"type\": {\n");
							++groupIndent;
						}
						groupString.append(indent(groupIndent)).append(getJsonNameValue("type", "array", true));
						groupString.append("\n");
						groupString.append(indent(groupIndent));
						groupString.append("\"items\": ");
						groupString.append("{\n");

						groupString.append(indent(++groupIndent)).append(getJsonNameValue("name", groupTypeName.concat("Item"), true));
						groupString.append("\n");
						groupString.append(indent(groupIndent)).append(getJsonNameValue("type", "record", true));
						groupString.append("\n");
						groupString.append(indent(groupIndent));
						groupString.append("\"fields\": [\n");

						final List<Object> componentMembers = groupType.getComponentRefOrGroupRefOrFieldRef();
						List<String> groupMemberStrings = new ArrayList<>();
						getMembersInline(
								componentMembers,
								namespace, 
								groups,
								components, 
								fields, 
								codeSets, 
								datatypes, 
								groupIndent,
								groupMemberStrings, 
								isNormaliseComponents, 
								isNormaliseGroups, 
								avroStandard);
						groupString.append(String.join(",\n", groupMemberStrings));
						groupString.append("\n");
						groupString.append(indent(groupIndent));
						groupString.append("]\n");

						groupString.append(indent(--groupIndent));
						groupString.append("}\n");
						
						groupString.append(indent(--groupIndent));
						groupString.append("}\n");
						if (!groupRefType.getPresence().equals(PresenceT.REQUIRED)) {
							groupString.append(indent(--groupIndent)).append("], \"default\": null\n");
						}

						groupString.append(indent(--groupIndent));
						groupString.append("}");	
						memberStrings.add(groupString.toString());
					}
				} else {
					System.err.format("writeMembersInline : Group missing from repository; id=%d%n", id);
				}
			} else if (member instanceof ComponentRefType) {
				final ComponentRefType componentRefType = (ComponentRefType) member;
				final int id = componentRefType.getId().intValue();
				final ComponentType componentType = components.get(id);
				if (componentType != null) {
					if (isNormaliseComponents) {
						String componentTypeName = componentType.getName();
						final String memberName = SchemaGeneratorUtil.firstCharToLowerCase(componentTypeName);
						final String generatedType = namespace.concat(".").concat(COMPONENT_DIR).concat(".").concat(componentTypeName);
						memberStrings.add(getComponentInlineString(componentRefType, componentType, memberName, generatedType));
					} else {
						final List<Object> componentMembers = componentType.getComponentRefOrGroupRefOrFieldRef();
						getMembersInline(
								componentMembers,
								namespace, 
								groups, 
								components, 
								fields, 
								codeSets, 
								datatypes, 
								indent,
								memberStrings, 
								isNormaliseComponents, 
								isNormaliseGroups, 
								avroStandard);
					}
				} else {
					System.err.format("writeMembersInline : Component missing from repository; id=%d%n", id);
				}
			}
		}
	}

	private static String getComponentInlineString(ComponentRefType componentRefType, ComponentType componentType, String name, String type) {
		StringBuffer result = new StringBuffer();
		result.append(indent(2));
		result.append("{");
		result.append(SchemaGeneratorUtil.getJsonNameValue("name", name, true));
		if (componentRefType.getPresence().equals(PresenceT.REQUIRED)) {
			result.append(SchemaGeneratorUtil.getJsonNameValue("type", type, true));
		} else {
			result.append("\"type\": [ \"null\", ");
			result.append("\"").append(type).append("\"");
			result.append("], ");
			result.append("\"default\": null, ");
		}

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
		if (groupRefType.getPresence().equals(PresenceT.REQUIRED)) {
			result.append(SchemaGeneratorUtil.getJsonNameValue("type", type, true));
		} else {
			result.append("\"type\": [ \"null\", ");
			result.append("\"").append(type).append("\"");
			result.append("], ");
			result.append("\"default\": null, ");
		}
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

	private static void generateCodeSet(String namespace, final String name,
			final CodeSetType codeSet, final String fixType, final File typeFile) throws IOException {
		try (Writer writer = new OutputStreamWriter(new FileOutputStream(typeFile), StandardCharsets.UTF_8)) {
			SchemaGeneratorUtil.writeOpenBracket(writer);
			SchemaGeneratorUtil.writeName(writer, name, 1);
			SchemaGeneratorUtil.writeNameSpace(writer, namespace, CODE_SET_DIR);
			SchemaGeneratorUtil.writeEnumDef(writer);
			List<CodeType> codes = codeSet.getCode();
			String unknown = "UNKNOWN_".concat(SchemaGeneratorUtil.precedeCapsWithUnderscore(codeSet.getName().replaceAll("CodeSet$", "")));
			for (CodeType code : codes) {
				writer.write(indent(3));
				writer.write("\"");
				writer.write(SchemaGeneratorUtil.precedeCapsWithUnderscore(code.getName()));
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

	private void generateGroup(File groupDir, GroupType group, String namespace,
			Map<Integer, GroupType> groups, Map<Integer, ComponentType> components,
			Map<Integer, FieldType> fields, Map<String, CodeSetType> codeSets, Map<String, Datatype> datatypes) throws IOException {
		final String name = SchemaGeneratorUtil.toTitleCase(group.getName());
		final File file = getFilePath(groupDir, name);
		try (Writer writer = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8)) {
			SchemaGeneratorUtil.writeOpenBracket(writer);
			SchemaGeneratorUtil.writeName(writer, name, 1);
			SchemaGeneratorUtil.writeNameSpace(writer, namespace, GROUP_DIR);
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(1), "type", "record");
			writer.write("\n");

			List<Object> docMembers = group.getAnnotation().getDocumentationOrAppinfo();
			List<String> docs = new ArrayList<>();
			SchemaGeneratorUtil.getDocumentationStrings(docMembers, docs);

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
			
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(5), "name", name.concat("Item"), true);
			writer.write("\n");
			SchemaGeneratorUtil.writeJsonNameValue(writer, indent(5), "type", "record");
			writer.write("\n");
			writer.write(indent(5));
			writer.write("\"fields\": [\n");
			final List<Object> members = group.getComponentRefOrGroupRefOrFieldRef();
			List<String> memberStrings = new ArrayList<>();
			getMembersInline(members, namespace, groups, components, fields, codeSets, datatypes, 5, memberStrings, this.isNormaliseComponents, this.isNormaliseGroups, this.avroStandard);
			writer.write(String.join(",\n", memberStrings));
			writer.write("\n");
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

	private static Repository unmarshal(InputStream inputFile) throws JAXBException {
		final JAXBContext jaxbContext = JAXBContext.newInstance(Repository.class, LogicalType.class);
		final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		return (Repository) jaxbUnmarshaller.unmarshal(inputFile);
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

	public boolean isNormaliseComponents() {
		return isNormaliseComponents;
	}

	public void setNormaliseComponents(boolean isNormaliseComponents) {
		this.isNormaliseComponents = isNormaliseComponents;
	}

	public boolean isNormaliseGroups() {
		return isNormaliseGroups;
	}

	public void setNormaliseGroups(boolean isNormaliseGroups) {
		this.isNormaliseGroups = isNormaliseGroups;
	}

	public void setAvroStandard(String avroStandard) {
		this.avroStandard = avroStandard;
	}
}
