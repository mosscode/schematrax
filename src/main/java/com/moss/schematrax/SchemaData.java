/**
 * Copyright (C) 2013, Moss Computing Inc.
 *
 * This file is part of schematrax.
 *
 * schematrax is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * schematrax is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with schematrax; see the file COPYING.  If not, write to the
 * Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 *
 * Linking this library statically or dynamically with other modules is
 * making a combined work based on this library.  Thus, the terms and
 * conditions of the GNU General Public License cover the whole
 * combination.
 *
 * As a special exception, the copyright holders of this library give you
 * permission to link this library with independent modules to produce an
 * executable, regardless of the license terms of these independent
 * modules, and to copy and distribute the resulting executable under
 * terms of your choice, provided that you also meet, for each linked
 * independent module, the terms and conditions of the license of that
 * module.  An independent module is a module which is not derived from
 * or based on this library.  If you modify this library, you may extend
 * this exception to your version of the library, but you are not
 * obligated to do so.  If you do not wish to do so, delete this
 * exception statement from your version.
 */
package com.moss.schematrax;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.moss.jdbcdrivers.DatabaseType;

/**
 * A SchemaData is an object model which represents the schema data stored on the 
 * classpath (e.g. index.xml, the updates, etc).
 */
class SchemaData {
	
	private static final Log log = LogFactory.getLog(SchemaData.class);
	
	private static final String 
			UPDATE_TAG_NAME="update", 
			VERSION_TAG_NAME="version", 
			VERSION_TAG_PRIOR_VERSION_ATTRIBUTE="prior_version",
			VERSION_TAG_CREATE_SCRIPT_ATTRIBUTE="create_script",
			APPLY_UPDATE_TAG_NAME="apply_update",
			ROOT_TAG_CURRENT_VERSION_ATTRIBUTE="currentVersion",
			CONFIGURATION_TAG_NAME = "configuration",
			DISABLE_STATEMENT_DELIMITER_PARSING_TAG_NAME = "disable-statment-delimiter-parsing",
			DATABASE_TYPE_TAG_NAME = "dbtype", 
			DATABASE_TYPE_NAME_ATTRIBUTE = "name"
				;
	
	private String resourcesPrefix;
	private ClassLoader loader;
	private SchemaUpdate[] updates;
	private SchemaVersion[] versions;
	private SchemaVersion currentVersion;
	
	public SchemaData(ClassLoader loader, String resourcesPrefix) throws Exception{
		this.resourcesPrefix = resourcesPrefix;
		this.loader = loader;
		
		Document document;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		String indexResourceLocation = resourcesPrefix + "index.xml";
		InputStream indexStream = loader.getResourceAsStream(indexResourceLocation);
		if(indexStream==null) throw new Exception("Could not find " + indexResourceLocation + " on the classpath.");
		document = builder.parse(indexStream);
		
		readConfiguration(document);
		initUpdates(document);
		
		initVersions(document);
		 
	}
	private String[] dbTypesForWhichToIgnoreDelimiters;
	
	private void readConfiguration(Document document){
		Node configNode = getFirstChildWithName(document.getDocumentElement(), CONFIGURATION_TAG_NAME);
		Node delimiterNode = getFirstChildWithName(configNode, DISABLE_STATEMENT_DELIMITER_PARSING_TAG_NAME);
		Node[] dbtypesTags = getChildrenWithName(delimiterNode, DATABASE_TYPE_TAG_NAME);
		dbTypesForWhichToIgnoreDelimiters = new String[dbtypesTags.length];
		for (int i = 0; i < dbtypesTags.length; i++) {
			dbTypesForWhichToIgnoreDelimiters[i] = dbtypesTags[i].getAttributes().getNamedItem(DATABASE_TYPE_NAME_ATTRIBUTE).getNodeValue();
			log.info("Ignoring delimiters for " + dbTypesForWhichToIgnoreDelimiters[i]);
		}
	}
	
	
	public boolean ignoreDelimitersForType(DatabaseType type){
		for (int x = 0; x < dbTypesForWhichToIgnoreDelimiters.length; x++) {
			String typeName = dbTypesForWhichToIgnoreDelimiters[x];
			if(typeName.equals(type.getShortName())) return true;
		}
		return false;
	}
	
	private Node getFirstChildWithName(Node node, String name){
		NodeList children = node.getChildNodes();
		for(int x=0;x<children.getLength();x++){
			Node child = children.item(x);
			if(child.getNodeName().equals(name)) return child;
		}
		return null;
	}
	
	private Node[] getChildrenWithName(Node node, String name){
		List matches = new ArrayList();
		NodeList children = node.getChildNodes();
		for(int x=0;x<children.getLength();x++){
			Node child = children.item(x);
			if(child.getNodeName().equals(name)) matches.add(child);
		}
		return (Node[]) matches.toArray(new Node[0]);
	}
	
	private void initUpdates(Document document){
		NodeList nodes = document.getDocumentElement().getChildNodes();
		ArrayList updatesList = new ArrayList();
		for(int x=0;x<nodes.getLength();x++){
			Node updateTag = nodes.item(x);
			if(updateTag.getNodeName().equals(UPDATE_TAG_NAME)){
				Node updateId = updateTag.getAttributes().getNamedItem("id");

				Node classNameAttribute = updateTag.getAttributes().getNamedItem("class");
				if (classNameAttribute != null) {
					updatesList.add(new DynamicSchemaUpdate(updateId.getNodeValue(), loader, classNameAttribute.getNodeValue()));
				}
				else {
					List<String> aliases = new ArrayList<String>();
					NodeList updateTagChildren = updateTag.getChildNodes();
					for(int y=0;y<updateTagChildren.getLength();y++){
						Node node = updateTagChildren.item(y);
						if(node.getNodeName().equals("alias")){
							aliases.add(node.getTextContent());
						}
					}
					updatesList.add(new LazyLoadingSchemaUpdate(updateId.getNodeValue(), aliases.toArray(new String[]{})));
				}
			}
		}
		updates = (SchemaUpdate[]) updatesList.toArray(new SchemaUpdate[0]);
	}
	
	private void initVersions(Document document) throws MissingElementException{
		
		NamedNodeMap attributes = document.getDocumentElement().getAttributes();
		String currentVersionId = attributes.getNamedItem(ROOT_TAG_CURRENT_VERSION_ATTRIBUTE).getNodeValue();
		
		NodeList nodes = document.getElementsByTagName(VERSION_TAG_NAME);
		versions = new SchemaVersion[nodes.getLength()];
		for(int x=0;x<nodes.getLength();x++){
			Node versionTag = nodes.item(x);
			Node versionId = versionTag.getAttributes().getNamedItem("id");
			LazyLoadingSchemaVersion version = new LazyLoadingSchemaVersion(versionId.getNodeValue());
			
			// Set the prior_version
			Node priorVersionAttribute = versionTag.getAttributes().getNamedItem(VERSION_TAG_PRIOR_VERSION_ATTRIBUTE);
			if(priorVersionAttribute!=null) version.setPriorVersionId(priorVersionAttribute.getNodeValue());
			
			Node createScriptAttribute = versionTag.getAttributes().getNamedItem(VERSION_TAG_CREATE_SCRIPT_ATTRIBUTE);
			if(createScriptAttribute!=null) version.setCreationScript(getUpdate(createScriptAttribute.getNodeValue()));
			
			
			versions[x] = version;
			
			NodeList children = versionTag.getChildNodes();
			for(int y=0;y<children.getLength();y++){
				Node child = children.item(y);
				if(child.getNodeName().equals(APPLY_UPDATE_TAG_NAME)){
					String updateId = child.getAttributes().getNamedItem("update_id").getNodeValue();
					SchemaUpdate update = getUpdate(updateId);
					version.addUpdate(update);
				}
			}
			
		}
		
		if(currentVersionId !=null){
			for (int x = 0; x < versions.length; x++) {
				SchemaVersion version = versions[x];
				if(version.getId().equals(currentVersionId)){
					this.currentVersion = version;
				}
			}
			
		}
		
	}
	
	public SchemaUpdate[] getUpdates() {
		 return updates;
	}
	
	public SchemaVersion getVersion(String id) throws VersionNotFoundException{
		for (int x = 0; x < versions.length; x++) {
			SchemaVersion version = versions[x];
			if(version.getId().equals(id))return version;
		}
		throw new VersionNotFoundException(id);
	}
	
	private SchemaUpdate getUpdate(String id) throws UpdateNotFoundException{
		for (int x = 0; x < updates.length; x++) {
			SchemaUpdate update = updates[x];
			if(update.getId().equals(id))return update;
		}
		throw new UpdateNotFoundException(id);
	}
	
	public SchemaVersion[] getVersions() {
		return versions;
	}
	
	public SchemaVersion getCurrentVersion(){
		return currentVersion;
	}
	private InputStream getUpdate(String id, DatabaseType dbType) throws DatabaseTypeNotSupportedException{
		InputStream in = loader.getResourceAsStream(resourcesPrefix + id + "/" + dbType.getShortName() + ".sql");
		if(in == null) in = loader.getResourceAsStream(resourcesPrefix + id + "/generic.sql");
		if(in == null) throw new DatabaseTypeNotSupportedException("Could not find a version of update \"" + id + "\" valid for database platform \"" + dbType.getShortName() + "\" (" + dbType.getPrettyName() + ")");
		return in;
	}
	
	class LazyLoadingSchemaUpdate implements SchemaUpdate{
		private String id;
		private String[] aliases;
		
		public LazyLoadingSchemaUpdate(String id, String[] aliases) {
			this.id = id;
			this.aliases = aliases;
		}
		
		public Reader getUpdateText(DatabaseType databaseType) throws DatabaseTypeNotSupportedException{
			Reader reader = new InputStreamReader(getUpdate(id, databaseType));
			return reader;
		}

		public String getId() {
			return id;
		}
		
		public String[] getAliases() {
			return aliases;
		}
		
	}
	
	class LazyLoadingSchemaVersion implements SchemaVersion{
		private String id;
		private ArrayList updates = new ArrayList();
		
		public LazyLoadingSchemaVersion(String id){
			this.id = id;
		}
		
		public void addUpdate(SchemaUpdate update){
			updates.add(update);
		}
		
		public String getId() {
			return id;
		}

		public SchemaUpdate[] getUpdates() {
			return (SchemaUpdate[]) updates.toArray(new SchemaUpdate[0]);
		}
		SchemaUpdate creationScript;
		public SchemaUpdate getCreationScript() {
			return creationScript;
		}
		
		String priorVersionId;
		void setPriorVersionId(String id){
			this.priorVersionId = id;
		}
		SchemaVersion priorVersion;
		public SchemaVersion getPriorVersion() {
			if(priorVersion==null && priorVersionId!=null){
				try {
					return getVersion(priorVersionId);
				} catch (VersionNotFoundException e) {
					e.printStackTrace();
				}
			}
			return priorVersion;
		}

		public void setCreationScript(SchemaUpdate creationScript) {
			this.creationScript = creationScript;
		}
		
	}
}
