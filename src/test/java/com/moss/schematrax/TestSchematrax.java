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

import java.sql.Connection;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.moss.blankslate.BlankslateCatalogHandle;
import com.moss.blankslate.CatalogFactory;
import com.moss.jdbcdrivers.DatabaseType;
import com.moss.schematrax.SchemaUpdater;

public class TestSchematrax extends TestCase {
	private Log log = LogFactory.getLog(TestSchematrax.class);
	private SchematraxExecutionPattern[] executionPatterns = new SchematraxExecutionPattern[]{
			new ExecutionPatternA(),
			new ExecutionPatternB(),
			new ExecutionPatternC(),
			new ExecutionPatternD()
	};
	
	public void testDerby() {
		runFullGauntlet(DatabaseType.DB_TYPE_DERBY, true);
		runFullGauntlet(DatabaseType.DB_TYPE_DERBY, false);
	}
	
	public void testHsqldb() {
		runFullGauntlet(DatabaseType.DB_TYPE_HSQLDB, true);
		runFullGauntlet(DatabaseType.DB_TYPE_HSQLDB, false);
	}
	
	public void testPostgresql() {
		runFullGauntlet(DatabaseType.DB_TYPE_POSTGRESQL, true);
		runFullGauntlet(DatabaseType.DB_TYPE_POSTGRESQL, false);
	}
	
	public void testMsSqlServer() {
		runFullGauntlet(DatabaseType.DB_TYPE_SQL_SERVER, true);
		runFullGauntlet(DatabaseType.DB_TYPE_SQL_SERVER, false);
	}
	
	private void runGauntlet(DatabaseType dbType, String prefix, boolean useManagedTransactions) throws Exception {
		
		SchemaUpdater updater = new SchemaUpdater(prefix, !useManagedTransactions);
		CatalogFactory factory = CatalogFactory.getFactory(dbType);
		
		for (SchematraxExecutionPattern executionPattern : executionPatterns) {
			executionPattern.runAndCleanup(factory, updater, dbType, useManagedTransactions);
		}
		
	}
	
	private void runFullGauntlet(DatabaseType dbType, boolean useManagedTransactions){
		try {
			runGauntlet(dbType, "com/moss/schematrax/test/schema1/", useManagedTransactions);
		} catch (Exception e) {
			e.printStackTrace();
			super.fail(e.getMessage());
		}
		
		try {
			runGauntlet(dbType, "com/moss/schematrax/test/schema2/", useManagedTransactions);
			super.fail("Errors built-in to schema2 were not propagated through the api");
		} catch (Exception e) {
			// this is expected because schema1 has errors in it
		}
		try {
			SchematraxExecutionPattern[] executionPatterns = new SchematraxExecutionPattern[]{
					new ExecutionPatternB(),
					new ExecutionPatternC(),
					new ExecutionPatternD()
			};
			
			CatalogFactory factory = CatalogFactory.getFactory(dbType);
			for (SchematraxExecutionPattern executionPattern : executionPatterns) {
				BlankslateCatalogHandle handle = factory.createCatalog();
				SchemaUpdater updater = new SchemaUpdater("com/moss/schematrax/test/schema3/", !useManagedTransactions);
				executionPattern.run(handle, updater, dbType, useManagedTransactions);
				updater = new SchemaUpdater("com/moss/schematrax/test/schema4/", !useManagedTransactions);
				executionPattern.run(handle, updater, dbType, useManagedTransactions);
				deleteCatalog(dbType, factory, handle);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			super.fail(e.getMessage());
		}
	}
	
	private abstract class SchematraxExecutionPattern {
		
		SchematraxExecutionPattern(){
		}
		final void runAndCleanup(CatalogFactory factory, SchemaUpdater updater, DatabaseType dbType, boolean useManagedTransactions) throws Exception{
			BlankslateCatalogHandle handle = factory.createCatalog();
			run(handle, updater, dbType, useManagedTransactions);
			deleteCatalog(dbType, factory, handle);
		}
		
		abstract void run(BlankslateCatalogHandle handle, SchemaUpdater updater, DatabaseType dbType, boolean useManagedTransactions) throws Exception ;
	}
	
	class ExecutionPatternA extends SchematraxExecutionPattern{
		

		@Override
		void run(BlankslateCatalogHandle handle, SchemaUpdater updater, DatabaseType dbType, boolean useManagedTransactions) throws Exception {
			Connection connection = handle.getConfig().createConnection();
			if(useManagedTransactions) connection = new MockJTAConnectionWrapper(connection);
			updater.createSchema(dbType, connection, handle.getDefaultSchemaName());
			updater.updateSchema(dbType, connection, handle.getDefaultSchemaName());
			updater.createOrUpdateSchema(dbType, connection, handle.getDefaultSchemaName());
			updater.createOrUpdateSchema(dbType, connection, handle.getDefaultSchemaName());
			if(useManagedTransactions) ((MockJTAConnectionWrapper)connection).doMockJTACommit();
			connection.close();
		}
		
	}
	
	class ExecutionPatternB extends SchematraxExecutionPattern{

		@Override
		void run(BlankslateCatalogHandle handle, SchemaUpdater updater, DatabaseType dbType, boolean useManagedTransactions) throws Exception {
			Connection connection = handle.getConfig().createConnection();
			if(useManagedTransactions) connection = new MockJTAConnectionWrapper(connection);
			updater.createOrUpdateSchema(dbType, connection, handle.getDefaultSchemaName());
			updater.updateSchema(dbType, connection, handle.getDefaultSchemaName());
			updater.createOrUpdateSchema(dbType, connection, handle.getDefaultSchemaName());
			if(useManagedTransactions) ((MockJTAConnectionWrapper)connection).doMockJTACommit();
			connection.close();
		}	
	}
	
	class ExecutionPatternC extends SchematraxExecutionPattern{

		@Override
		void run(BlankslateCatalogHandle handle, SchemaUpdater updater, DatabaseType dbType, boolean useManagedTransactions) throws Exception {
			Connection connection = handle.getConfig().createConnection();
			if(useManagedTransactions) connection = new MockJTAConnectionWrapper(connection);
			updater.createOrUpdateSchema(dbType, connection, handle.getDefaultSchemaName());
			updater.updateSchema(dbType, connection, handle.getDefaultSchemaName());
			updater.createOrUpdateSchema(dbType, connection, handle.getDefaultSchemaName());
			if(useManagedTransactions) ((MockJTAConnectionWrapper)connection).doMockJTACommit();
			connection.close();

		}	
	}
	
	class ExecutionPatternD extends SchematraxExecutionPattern{

		@Override
		void run(BlankslateCatalogHandle handle, SchemaUpdater updater, DatabaseType dbType, boolean useManagedTransactions) throws Exception {
			Connection connection = handle.getConfig().createConnection();
			if(useManagedTransactions) connection = new MockJTAConnectionWrapper(connection);
			updater.createOrUpdateSchema(dbType, connection, handle.getDefaultSchemaName());
			updater.updateSchema(dbType, connection, handle.getDefaultSchemaName());
			updater.createOrUpdateSchema(dbType, connection, handle.getDefaultSchemaName());
			if(useManagedTransactions) ((MockJTAConnectionWrapper)connection).doMockJTACommit();
			connection.close();
		}
	}
	
	/**
	 * This is just a hack to work-around the known problems with postgres catalog deletion
	 */
	private void deleteCatalog(DatabaseType dbType, CatalogFactory factory, BlankslateCatalogHandle handle) throws Exception {
		try {
			factory.deleteCatalog(handle);
		} catch (Exception e) {
			if(dbType==DatabaseType.DB_TYPE_POSTGRESQL){
				log.warn("There was an error deleting a postgres catalog - this is probably just an instance of the known issue of flakiness when deleting postgres catalogs.  The full stacktrace will follow as a DEBUG-level message.  The message received was:" + e.getMessage());
				log.debug("This is the aforementioned postgres error", e);
			}else{
				throw e;
			}
			
		}
	}
}
