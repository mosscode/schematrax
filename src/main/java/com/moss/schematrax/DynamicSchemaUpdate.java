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

import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.moss.jdbcdrivers.DatabaseType;
import com.moss.schematrax.dynamic.JavaSchemaUpdate;
import com.moss.schematrax.dynamic.JavaUpdateContext;
import com.moss.schematrax.dynamic.SqlBlockParser;
import com.moss.schematrax.dynamic.SupportedDatabases;

/**
 * Wraps and handles the loading and executing of {@link JavaSchemaUpdate}s.
 */
class DynamicSchemaUpdate implements SchemaUpdate {
	
	private static final Log log = LogFactory.getLog(DynamicSchemaUpdate.class);

	private String updateId;
	private Class updateClass;
	private List<DatabaseType> supportedDatabases;
	
	private SqlBlockParser parser;
	private JavaUpdateContext ctx;
	
	public DynamicSchemaUpdate(String updateId, ClassLoader loader, String updateClassName) {
		this.updateId = updateId;
		
		try {
			updateClass = loader.loadClass(updateClassName);
			
			if (!JavaSchemaUpdate.class.isAssignableFrom(updateClass)) {
				throw new RuntimeException("Update classes must implement interface " + JavaSchemaUpdate.class.getName() + ": " + updateClass.getName());
			}
		}
		catch (ClassNotFoundException ex) {
			throw new RuntimeException ("Could not find update class " + updateClassName, ex);
		}
		catch (Exception ex) {
			throw new RuntimeException("Could not load update class: " + updateClassName, ex); 
		}
		
		try {
			updateClass.getConstructor(new Class[0]);
		}
		catch (NoSuchMethodException ex) {
			throw new RuntimeException ("Update classes must have a no-arg constructor " + updateClassName, ex);
		}
		
		SupportedDatabases supported = (SupportedDatabases)updateClass.getAnnotation(SupportedDatabases.class);
		if (supported == null) {
			throw new RuntimeException("Update classes must have a class level " + SupportedDatabases.class.getName() + " annotation: " + updateClass.getName());
		}
		
		supportedDatabases = new ArrayList<DatabaseType>();
		for (String dbId : supported.value()) {
			DatabaseType dbType = DatabaseType.getDatabaseType(dbId);
			if (dbType == null) {
				log.warn("Ignoring unknown supported database type " + dbId + " for update " + updateId + "(" + updateClass.getName() + ")");
			}
			else {
				supportedDatabases.add(dbType);
			}
		}
	}
	
	public String getId() {
		return updateId;
	}
	public String[] getAliases() {
		return new String[]{};
	}
	
	public void setSqlBlockParser(SqlBlockParser parser) {
		this.parser = parser;
	}
	
	public void setUpdateContext(JavaUpdateContext ctx) {
		this.ctx = ctx;
	}

	public Reader getUpdateText(DatabaseType databaseType) throws DatabaseTypeNotSupportedException {
		if (parser == null) {
			throw new RuntimeException("The " + SqlBlockParser.class.getName() + " for this class must be initialized before this method (getUpdateText()) is called.");
		}
		
		if (ctx == null) {
			throw new RuntimeException("The " + JavaUpdateContext.class.getName() + " for this class must be initialized before this method (getUpdateText()) is called.");
		}
		
		if (!supportedDatabases.contains(databaseType)) {
			throw new DatabaseTypeNotSupportedException("Could not find a version of update \"" + updateId + "\" valid for database platform \"" + databaseType.getShortName() + "\" (" + databaseType.getPrettyName() + ")");
		}
		
		JavaSchemaUpdate update = null;
		try {
			update = (JavaSchemaUpdate)updateClass.newInstance();
		}
		catch (Exception ex) {
			throw new RuntimeException("Cannot instantiate update class " + updateClass.getName() + " even though it appears to have a no-arg constructor!");
		}
		
		try {
			Reader sqlReader = update.performUpdate(ctx);
			return sqlReader;
		}
		catch (SQLException ex) {
			throw new RuntimeException("Wrapping an " + SQLException.class.getName() + " thrown while performing java update " + updateId, ex);
		}
		catch (Exception ex) {
			log.error("Error parsing sql block for dynamic update " + updateId + " (" + updateClass.getName() + ")", ex);
			return null;
		}
	}
}
