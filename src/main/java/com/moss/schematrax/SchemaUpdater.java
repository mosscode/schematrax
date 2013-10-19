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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.moss.jdbcdrivers.DatabaseType;
import com.moss.jdbcdrivers.JdbcConnectionConfig;
import com.moss.schematrax.dynamic.JavaUpdateContext;
import com.moss.schematrax.dynamic.SqlBlockParser;
import com.moss.schematrax.injection.ComplexInjection;
import com.moss.schematrax.injection.InjectedSchemaUpdate;
import com.moss.schematrax.injection.InjectionException;
import com.moss.schematrax.injection.Injector;
import com.moss.schematrax.injection.SchemaInjection;

/**
 * This is the core of schematrax.  Tools which desire to embed schematrax to work with schematrax files should
 * use this class.
 */
public class SchemaUpdater implements SqlBlockParser{
	private static final Log log = LogFactory.getLog(SchemaUpdater.class);
	
	private ClassLoader updatesLoader;
	boolean manageTransactions=true;
	
	private List<Injector> injectors = new ArrayList<Injector>();
	
	/**
	 * For use when the updates are already on the classpath
	 */
	public SchemaUpdater() throws Exception {
		this("");
	}
	
	public SchemaUpdater(boolean manageTransactions) throws Exception {
		this("", manageTransactions);
	}
	/**
	 * For use when the updates are already on the classpath
	 */
	public SchemaUpdater(String prefix) throws Exception {
		this(prefix, true);
	}
	
	public SchemaUpdater(String prefix, boolean manageTransactions) throws Exception {
		this.manageTransactions = manageTransactions;
		if(prefix.equals("")) log.info("Starting SchemaUpdater with default classpath");
		else log.info("Starting SchemaUpdater with classpath root: " + prefix);
		
		updatesLoader = new URLClassLoader(new URL[]{}, this.getClass().getClassLoader());
		init(prefix);
	}
	/**
	 * For use when the updates are already on the classpath, and
	 * the classloader that loaded schematrax is not the same as the
	 * class loader that contains the schematrax resources.
	 */
	public SchemaUpdater(ClassLoader cl, String prefix) throws Exception {
		if(prefix.equals("")) log.info("Starting SchemaUpdater with default classpath");
		else log.info("Starting SchemaUpdater with classpath root: " + prefix);
		
		updatesLoader = cl;
		init(prefix);
	}
	
	/**
	 * For use when the updates are not already on the classpath
	 * @param updatesLocation the location of the schematrax file to load
	 */
	public SchemaUpdater(URL updatesLocation) throws Exception{
		URL[] locations = {updatesLocation};
		updatesLoader = new URLClassLoader(locations, null);
		
		String classpathRoot = "";
		InputStream propertiesData = updatesLoader.getResourceAsStream("schematrax.properties");
		if(propertiesData !=null) {
			Properties props = new Properties();
			props.load(propertiesData);
			String root = props.getProperty("classpath-root");
			if(root!=null) classpathRoot = root;
		}
		
		init(classpathRoot);
	}
	
	private void init(String classpathRoot) throws Exception {
		schemaData = new SchemaData(updatesLoader, classpathRoot);
	}
	
	/**
	 * Parses a block of sql into individual sql statements that can be consistently
	 * executed by schematrax across different database vendors. Takes into account
	 * the database vendor when parsing.
	 */

	public String[] parseSqlBlock(Reader r, DatabaseType databaseType) throws IOException {
		BufferedReader reader = new BufferedReader(r);
		
		// An update can have multiple statements
		List<String> updateStatements = new ArrayList<String>();
		
		StringBuffer sb = new StringBuffer();
		for(String line = reader.readLine();line!=null;line= reader.readLine()){
			if(!line.startsWith("//") && !line.startsWith("--")){ //parse non-comment lines
				if(line.trim().endsWith(";") && !schemaData.ignoreDelimitersForType(databaseType)){
					line = trimTrailingWhitespace(line); //remove the trailing whitespace (keeping the leading whitespace, otherwise I'd just use String.trim()
					line = line.substring(0, line.length()-1); //remove the delimiter char
					sb.append(line);
					sb.append("\n"); //add a newline since readLine() takes them out
					
					// Since the statement has ended, record it and reset the buffer for future statements
					updateStatements.add(sb.toString());
					sb = new StringBuffer();
				}else{
					sb.append(line);
					sb.append("\n"); //add a newline since readLine() takes them out
				}
				
			}
		}
		String trimmedTextInBuffer = sb.toString().trim();
		if(!trimmedTextInBuffer.equals("")){
			updateStatements.add(sb.toString());
		}
		return (String[]) updateStatements.toArray(new String[0]);
	}

	private String trimTrailingWhitespace(String string){
		int position = string.length()-1; 
		while(position >=0 && Character.isWhitespace(string.charAt(position))) position--;
		return string.substring(0, position+1);
	}
	
	public void add(Injector l) {
		if (!injectors.contains(l)) {
			injectors.add(l);
		}
	}
	
	public void remove(Injector l) {
		injectors.remove(l);
	}
	
	public synchronized void createOrUpdateSchema(DatabaseType databaseType, JdbcConnectionConfig connectionConfig, String schema ) throws SchematraxException {
		try {
			Class.forName(connectionConfig.getJdbcDriverClassName());
			Connection sqlConnection = DriverManager.getConnection(connectionConfig.getJdbcUrl(), connectionConfig.getLogon(), connectionConfig.getPassword());
			sqlConnection.setAutoCommit(false);
			
			createOrUpdateSchema(databaseType, sqlConnection, schema);
			
			sqlConnection.close();
		} catch (ClassNotFoundException e) {
			throw new SchematraxException(e);
		} catch (SQLException e) {
			throw new SchematraxException(e);
		}
	}
	
	public synchronized void createOrUpdateSchema(DatabaseType databaseType, Connection sqlConnection, String schema ) throws SchematraxException {
		
		try {
			if(!updatesTableExists(databaseType, sqlConnection, schema)){
				log.info("SCHEMA_UPDATES table not found - this must be a new/blank schema - running the creation script.");
				createSchema(databaseType, sqlConnection, schema, getCurrentVersionId());
			}else{
				log.info("Existing SCHEMA_UPDATES table found - treating this as an existing schema, running the update script.");
				updateSchema(databaseType, sqlConnection, schema, getCurrentVersionId());
			}
		} catch (SQLException e) {
			throw new SchematraxException(e);
		} catch (ClassNotFoundException e) {
			throw new SchematraxException(e);

		}
	}
	
	private boolean tableExists(Connection sqlConnection, String schema,  String tableName) throws SQLException {
		boolean tableExists = false;
		ResultSet tables = sqlConnection.getMetaData().getTables(null, schema, tableName, null);
		while(tables.next()){
			String nextTableName = tables.getString("TABLE_NAME");
			if(nextTableName!=null && nextTableName.equals(tableName)) tableExists=true;
		}
		return tableExists;
	}
	
	private boolean updatesTableExists(DatabaseType databaseType, Connection sqlConnection, String schema ) throws SQLException, ClassNotFoundException{
		boolean updatesTableExists=false;
		
		updatesTableExists = tableExists(sqlConnection, schema, "SCHEMA_UPDATES");
		
		// THIS IS NECESSARY FOR POSTGRES (at least), SINCE IT LIKES TO CONVERT NAMES TO LOWER CASE
		if(databaseType == DatabaseType.DB_TYPE_POSTGRESQL && !updatesTableExists){
			updatesTableExists = tableExists(sqlConnection, schema.toLowerCase(), "schema_updates");
		}
		
		if(databaseType == DatabaseType.DB_TYPE_HSQLDB && !updatesTableExists){
			try {
				sqlConnection.createStatement().execute("select * from " + schema + ".SCHEMA_UPDATES");
				updatesTableExists = true;
			} catch (SQLException e) {
			}
			
		}
		return updatesTableExists;
	}
	
	public synchronized void createSchema(DatabaseType databaseType, JdbcConnectionConfig connectionConfig, String schema, String schemaVersion) throws SchematraxException {
		try {
			Class.forName(connectionConfig.getJdbcDriverClassName());
			Connection sqlConnection = DriverManager.getConnection(connectionConfig.getJdbcUrl(), connectionConfig.getLogon(), connectionConfig.getPassword());
			
			createSchema(databaseType, sqlConnection, schema, schemaVersion);
			
			sqlConnection.close();
		} catch (ClassNotFoundException e) {
			throw new SchematraxException(e);
		} catch (SQLException e) {
			throw new SchematraxException(e);

		}
	}
	
	public synchronized void createSchema(DatabaseType databaseType, Connection sqlConnection, String schema) throws SchematraxException {
		createSchema(databaseType, sqlConnection, schema, getCurrentVersionId());
	}
	
	public synchronized void createSchema(DatabaseType databaseType, Connection sqlConnection, String schema, String schemaVersion) throws SchematraxException {
		
		try {
			/*
			 * CREATE THE SCHEMA_UPDATES TABLE
			 */
			if(manageTransactions) sqlConnection.setAutoCommit(false);
			boolean updatesTableExists=updatesTableExists(databaseType, sqlConnection, schema);
			
			if(!updatesTableExists){
				Statement statement = sqlConnection.createStatement();
				String timestampDataType = "TIMESTAMP";
				
				if(databaseType.equals(DatabaseType.DB_TYPE_SQL_SERVER)) {
					timestampDataType = "DATETIME";
				}
				
				String createUpdatesTableStatementText = "create table " + schema + ".SCHEMA_UPDATES (id varchar(255), dateApplied " + timestampDataType + ")";
				statement.executeUpdate(createUpdatesTableStatementText);
				if(manageTransactions) sqlConnection.commit();
			}
			
			/*
			 * FIND AND APPLY THE CREATION SCRIPT
			 */
			SchemaVersion priorVersion = schemaData.getVersion(schemaVersion);
			SchemaUpdate creationScript=null;
			
			while(creationScript==null & priorVersion!=null){
				try {
					SchemaUpdate versionCreateScript = priorVersion.getCreationScript();
					if(versionCreateScript!=null){
						/* 
						 * This will throw an exception if this doesn't exist for our 
						 * current platform (which we can ignore since there may be an 
						 * earlier script we can use as a starting point)
						 */
						versionCreateScript.getUpdateText(databaseType);
						creationScript=versionCreateScript;
					}
				} catch (DatabaseTypeNotSupportedException e) {
				}
				
				priorVersion = priorVersion.getPriorVersion();
			}
			if(creationScript == null){
				throw new MissingElementException("Could not find a schema creation script");
			}
			
			log.info("Executing creation script");
			applyUpdateNoCommit(sqlConnection, databaseType, schema, creationScript);
			recordUpdate(sqlConnection, schema, creationScript);
			
			
			/*
			 * HANDLE CONSOLIDATED UPDATES
			 */
			List<SchemaUpdate> consolidatedUpdates = getUpdatesToVersion(priorVersion);
			List<String> consolidatedUpdateIds = new ArrayList<String>();
			for (SchemaUpdate update : consolidatedUpdates) {
				consolidatedUpdateIds.add(update.getId());
				log.info("Marking consolidated update \"" + update.getId() + "\" as applied.");
				recordUpdate(sqlConnection, schema, update); // we record these because they are rolled-into the creation script
			}
			
			//			 Apply injections
			for (Injector injector : injectors) {
				String description = "after update '" + creationScript.getId() + "', injected by " + Injector.class.getSimpleName() + " " + injector.getClass().getName();
				doInjections(injector.forVersion(creationScript.getId(), consolidatedUpdateIds), databaseType, sqlConnection, schema, description);
			}
			
			// now run the normal schema update routine to get us up to the requested version as needed
			updateSchema( databaseType,  sqlConnection,  schema,  schemaVersion);
			
			if(manageTransactions) sqlConnection.commit();
			
		}catch (Exception e){
			log.info("There was an error", e);
			try {
				if(manageTransactions) sqlConnection.rollback();
				throw new SchematraxException(e);
			} catch (SQLException e1) {
				log.info("There was an error rolling back the transaction", e1);
				throw new SchematraxException(e1);
			}
		}
		
	}
	
	private List<SchemaUpdate> getUpdatesToVersion(SchemaVersion version){
		List<SchemaUpdate> earlierUpdates = new ArrayList<SchemaUpdate>();
		
		while(version!=null){
			earlierUpdates.addAll(Arrays.asList(version.getUpdates()));
			version = version.getPriorVersion();
		}
		
		return earlierUpdates;
	}
	
	private void applyUpdatesForVersion(SchemaVersion version, List appliedUpdates, DatabaseType databaseType, String schema, Connection sqlConnection) throws SQLException, IOException, DatabaseTypeNotSupportedException, SchemaUpdateException{
		log.info("UPDATING TO VERSION " + version.getId());
		
		//Make sure all of this version's updates are applied
		SchemaUpdate[] updates = version.getUpdates();
		for (int y = 0; y < updates.length; y++) {
			SchemaUpdate update = updates[y];
			boolean updateIsApplied = false;
			boolean updateMatchesAlias = false;
			String matchedAlias = null;
			
			for (Iterator i = appliedUpdates.iterator(); i
			.hasNext();) {
				AppliedSchemaUpdate appliedUpdate = (AppliedSchemaUpdate) i.next();
				if(appliedUpdate.getId().equals(update.getId())){
					updateIsApplied=true;
				}else{
					for (String alias : update.getAliases()) {
						if(appliedUpdate.getId().equals(alias)){
							updateMatchesAlias=true;
							matchedAlias = alias;
						}
						
					}
				}
			}
			
			if(!updateIsApplied && !updateMatchesAlias){
			
				// Apply the update itself
				applyUpdateNoCommit(sqlConnection, databaseType, schema, update);
				
				// Apply injections
				for (Injector injector : injectors) {
					String description = "after update '" + update.getId() + "', injected by " + Injector.class.getSimpleName() + " " + injector.getClass().getName();
					doInjections(injector.forVersion(update.getId(), new ArrayList<String>(0)), databaseType, sqlConnection, schema, description);
				}
				
				// Record the fact that it was updated
				recordUpdate(sqlConnection, schema, update);
				if(manageTransactions) sqlConnection.commit();
				
			}else{
				if(updateIsApplied)
					log.info("Skipping update " + update.getId());
				else if(updateMatchesAlias)
					log.info("Skipping update " + update.getId() + " (matching alias " + matchedAlias + ")");
			}
		}
	}
	
	
	private void doInjections(List<SchemaInjection> injections, DatabaseType databaseType, Connection sqlConnection, String schema, String injectionDescription) throws DatabaseTypeNotSupportedException, SchemaUpdateException {
		if (injections != null) {
			for (SchemaInjection injection : injections) {
				log.info("[INJECTION] Injecting '" + injection.getId() + "' " + injectionDescription );
				if(injection instanceof ComplexInjection){
					try {
						((ComplexInjection)injection).inject(sqlConnection);
					} catch (InjectionException e) {
						throw new SchemaUpdateException(e);
					}
				}else if(injection instanceof InjectedSchemaUpdate){
					SchemaUpdate processedUpdate = new ProcessedSchemaUpdate((InjectedSchemaUpdate)injection);

					applyUpdateNoCommit(sqlConnection, databaseType, schema, processedUpdate);

				}else throw new IllegalStateException("Unsupported Injection Type:" + injection.getClass());
			}
		}
	}
	
	private void applyUpdateNoCommit(Connection sqlConnection, DatabaseType databaseType, String schema, SchemaUpdate update) throws SchemaUpdateException, DatabaseTypeNotSupportedException {
		// Apply this update
		log.info("Applying update " + update.getId());
		
		if (update instanceof DynamicSchemaUpdate) {
			JavaUpdateContext ctx = new JavaUpdateContext();
			ctx.setConnection(sqlConnection);
			ctx.setSchemaName(schema);
			ctx.setDatabaseType(databaseType);
			
			DynamicSchemaUpdate dynamicUpdate = (DynamicSchemaUpdate)update;
			dynamicUpdate.setUpdateContext(ctx);
			dynamicUpdate.setSqlBlockParser(this);
		}
		
		try {
			Reader updateReader = update.getUpdateText(databaseType);
			
			String[] sqlStatementTexts = parseSqlBlock(updateReader, databaseType);
			
			Statement statement = sqlConnection.createStatement();
			
			for (int i = 0; i < sqlStatementTexts.length; i++) {
				String statementText = sqlStatementTexts[i];
				
				statementText = translateSqlStatement(schema, statementText);
				
				log.debug("Executing statement:\n" + statementText);
				try {
					statement.execute(statementText);
				} catch (SQLException e) {
					log.info("Passing an SQLException which was caught executing statement: " + statementText, e);
					throw new SchemaUpdateException(e);
				}
			}
		} catch (IOException e) {
			throw new SchemaUpdateException(e);
		} catch (SQLException e) {
			throw new SchemaUpdateException(e);
		}
	}
	
	private class ProcessedSchemaUpdate implements SchemaUpdate {

		private InjectedSchemaUpdate injectedUpdate;
		
		public ProcessedSchemaUpdate(InjectedSchemaUpdate injectedUpdate) {
			this.injectedUpdate = injectedUpdate;
		}
		
		public String getId() {
			return injectedUpdate.getId();
		}

		public Reader getUpdateText(DatabaseType databaseType) throws DatabaseTypeNotSupportedException {
			Reader reader = injectedUpdate.getSqlBlock();
			return reader;
		}
		
		public String[] getAliases() {
			return new String[]{};
		}
	}
	
	private void recordUpdate(Connection sqlConnection, String schema, SchemaUpdate update) throws SQLException{
		PreparedStatement preparedStatement = sqlConnection.prepareStatement("INSERT INTO " + schema + ".SCHEMA_UPDATES values (?,?)");
		preparedStatement.setString(1, update.getId());
		preparedStatement.setTimestamp(2, new java.sql.Timestamp(new Date().getTime()));
		preparedStatement.execute();
		if(manageTransactions) sqlConnection.commit();
	}
	
	public String[] listVersions(){
		SchemaVersion[] versions = schemaData.getVersions();
		String[] versionIds = new String[versions.length];
		for (int i = 0; i < versions.length; i++) {
			SchemaVersion version = versions[i];
			versionIds[i] = version.getId();
		}
		return versionIds;
	}
	
	public String getCurrentVersionId(){
		SchemaVersion version = schemaData.getCurrentVersion();
		
		if(version==null)return null;
		
		return version.getId();
	}
	
	private List listAppliedUpdates(Connection jdbcConnection, String schemaName) throws SQLException{
		List<AppliedSchemaUpdate> appliedUpdates = new ArrayList<AppliedSchemaUpdate>();
		ResultSet updates = jdbcConnection.createStatement().executeQuery("SELECT id, dateApplied FROM " + schemaName + ".SCHEMA_UPDATES");
		while(updates.next()){
			AppliedSchemaUpdate update = new AppliedSchemaUpdate();
			update.setId(updates.getString("id"));
			update.setDateApplied(updates.getDate("dateApplied"));
			appliedUpdates.add(update);
		}
		return appliedUpdates;
	}
	
	
	public void updateSchema(DatabaseType databaseType, JdbcConnectionConfig connectionConfig, String schema, String schemaVersion) throws SchematraxException {
		try {
			Class.forName(connectionConfig.getJdbcDriverClassName());
			Connection sqlConnection = DriverManager.getConnection(connectionConfig.getJdbcUrl(), connectionConfig.getLogon(), connectionConfig.getPassword());
			updateSchema(databaseType, sqlConnection, schema, schemaVersion);
			sqlConnection.close();
		} catch (ClassNotFoundException e) {
			throw new SchematraxException(e);
		} catch (SQLException e) {
			throw new SchematraxException(e);
		}
	}
	
	public void updateSchema(DatabaseType databaseType, Connection sqlConnection, String schema) throws SchematraxException {
		updateSchema(databaseType, sqlConnection, schema, getCurrentVersionId());
	}
	
	public void updateSchema(DatabaseType databaseType, Connection sqlConnection, String schema, String schemaVersion) throws SchematraxException {
		
		try {
			if(manageTransactions) sqlConnection.setAutoCommit(false);
			
			List appliedUpdates = listAppliedUpdates(sqlConnection, schema);
			/*
			 * PROCESS THE SQL UPDATES
			 */
			List<SchemaVersion> versionsList = new ArrayList<SchemaVersion>();
			// Create an array of all prior versions
			SchemaVersion version = schemaData.getVersion(schemaVersion);
			versionsList.add(version);
			while(version.getPriorVersion()!=null){
				version = version.getPriorVersion();
				versionsList.add(version);
			}
			
			for(int x = versionsList.size()-1;x>-1;x--){
				version = (SchemaVersion) versionsList.get(x);
				applyUpdatesForVersion(version, appliedUpdates, databaseType, schema, sqlConnection);
			}
			
			// WE'RE DONE! NOTIFY THE INJECTORS
			for (Injector injector : injectors) {
				injector.updateComplete();
			}
			
			if(manageTransactions) sqlConnection.commit();
		} catch (Exception e) {
			try {
				if(manageTransactions) sqlConnection.rollback();
				if(e instanceof SchematraxException) throw (SchematraxException) e;
				throw new SchemaUpdateException(e);
			} catch (SQLException e1) {
				log.fatal("An update failed.", e); // this is so that we still get the root cause somewhere
				throw new SchemaUpdateException(e1);
			}
		}
		
	}
	
	private static String SCHEMA_PATTERN="##SCHEMA##";
	private SchemaData schemaData;
	
	public String translateSqlStatement(String schema, String statementString){
		//next replace the schema name with the schema passed
		statementString = statementString.replaceAll(SCHEMA_PATTERN, schema);
		return statementString;
	}
	
	static final String usage = "Usage: \n" +
	"SchemaUpdater mode=create [schematraxFile=path] dbtype=(mssql|postgres|...) catalog=databaseName [schema=name] logon=logon password=password host=hostname\n" +
	"SchemaUpdater mode=update [schematraxFile=path] version=schemaVersion dbtype=(mssql|postgres|...) catalog=databaseName [schema=name] logon=logon password=password host=hostname "
	;
	public static void main(String[] args) throws Exception {
		
		boolean configureLogging = new Boolean(System.getProperty("configureLogging", "true")).booleanValue();
		if (configureLogging) {
			BasicConfigurator.configure();
			Logger.getRootLogger().setLevel(Level.INFO);
		}
		
		Map<String, String> argsMap = new HashMap<String, String>();
		for(int x=0;x<args.length;x++){
			String arg = args[x];
			String[] pair = arg.split("=");
			if(pair.length!=2) {
				System.out.println("Error parsing command line arguments in value pair:" + arg);
				System.out.println(usage);
				System.exit(1);
			}
			String name = pair[0];
			String value = pair[1];
			argsMap.put(name, value);
		}
		
		String mode = (String)argsMap.get("mode");
		String databaseTypeName = (String) argsMap.get("dbtype");
		String databaseName = (String) argsMap.get("catalog");
		String logon = (String) argsMap.get("logon");
		String password = (String) argsMap.get("password");
		String host = (String) argsMap.get("host");
		if(
				args.length<5
				||
				databaseTypeName == null
				||
				databaseName == null
				||
				logon==null
				||
				password==null
				||
				host==null
				||
				mode==null
		){
			System.out.println(usage);
		}
		
		
		String schematraxFileLocation = (String)argsMap.get("schematraxFile");
		
		if(schematraxFileLocation==null)schematraxFileLocation="";
		
		String schemaName = (String) argsMap.get("schema");
		if(schemaName==null) schemaName = logon;
		
		DatabaseType dbType = DatabaseType.getDatabaseType(databaseTypeName);
		
		String schemaVersion = (String) argsMap.get("version");
		
		
		
		JdbcConnectionConfig connectionConfig = new JdbcConnectionConfig();
		connectionConfig.setJdbcDriverClassName(dbType.getJdbcDriver().getClassName());
		connectionConfig.setJdbcUrl(dbType.getJdbcDriver().createJdbcUrl(host, null, databaseName, logon, password));
		connectionConfig.setLogon(logon);
		connectionConfig.setPassword(password);
		
		SchemaUpdater updater = new SchemaUpdater(new File(schematraxFileLocation).toURL());
		
		if(mode.equals("update")){
			if(schemaVersion==null){
				System.err.println("You must specify a schema version");
				System.err.println(usage);
			}
			updater.updateSchema(dbType, connectionConfig, schemaName,  schemaVersion);
		}else if(mode.equals("create")){
			updater.createSchema(dbType, connectionConfig, schemaName, schemaVersion);
		}else {
			System.err.println("Invalid mode:" + mode);
			System.err.println(usage);
		}
	}
	
}
