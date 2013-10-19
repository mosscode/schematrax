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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

public class MockJTAConnectionWrapper implements Connection {
	private Connection wrappedConnection;
	
	public MockJTAConnectionWrapper(Connection wrappedConnection) throws SQLException {
		this.wrappedConnection = wrappedConnection;
		wrappedConnection.setAutoCommit(false);
	}
	
	
	public void doMockJTACommit() throws SQLException {
		wrappedConnection.commit();
	}
	
	public void rollback() throws SQLException {
		throw new SQLException("rollback() cannot be called in a JTA managed connection");
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLException("rollback() cannot be called in a JTA managed connection");
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		throw new SQLException("setAutoCommit() cannot be called in a JTA managed connection");
	}
	
	public void commit() throws SQLException {
		throw new SQLException("commit() cannot be called in a JTA managed connection");
	}
	
	
	public void clearWarnings() throws SQLException {
		wrappedConnection.clearWarnings();
	}

	public void close() throws SQLException {
		wrappedConnection.close();
	}

	public Statement createStatement() throws SQLException {
		return wrappedConnection.createStatement();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return wrappedConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return wrappedConnection.createStatement(resultSetType, resultSetConcurrency);
	}

	public boolean getAutoCommit() throws SQLException {
		return wrappedConnection.getAutoCommit();
	}

	public String getCatalog() throws SQLException {
		return wrappedConnection.getCatalog();
	}

	public int getHoldability() throws SQLException {
		return wrappedConnection.getHoldability();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return wrappedConnection.getMetaData();
	}

	public int getTransactionIsolation() throws SQLException {
		return wrappedConnection.getTransactionIsolation();
	}

	public Map getTypeMap() throws SQLException {
		return wrappedConnection.getTypeMap();
	}

	public SQLWarning getWarnings() throws SQLException {
		return wrappedConnection.getWarnings();
	}

	public boolean isClosed() throws SQLException {
		return wrappedConnection.isClosed();
	}

	public boolean isReadOnly() throws SQLException {
		return wrappedConnection.isReadOnly();
	}

	public String nativeSQL(String sql) throws SQLException {
		return wrappedConnection.nativeSQL(sql);
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return wrappedConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return wrappedConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		return wrappedConnection.prepareCall(sql);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return wrappedConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return wrappedConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		return wrappedConnection.prepareStatement(sql, autoGeneratedKeys);
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		return wrappedConnection.prepareStatement(sql, columnIndexes);
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		return wrappedConnection.prepareStatement(sql, columnNames);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return wrappedConnection.prepareStatement(sql);
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		wrappedConnection.releaseSavepoint(savepoint);
	}

	public void setCatalog(String catalog) throws SQLException {
		wrappedConnection.setCatalog(catalog);
	}

	public void setHoldability(int holdability) throws SQLException {
		wrappedConnection.setHoldability(holdability);
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		wrappedConnection.setReadOnly(readOnly);
	}

	public Savepoint setSavepoint() throws SQLException {
		return wrappedConnection.setSavepoint();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		return wrappedConnection.setSavepoint(name);
	}

	public void setTransactionIsolation(int level) throws SQLException {
		wrappedConnection.setTransactionIsolation(level);
	}

	public void setTypeMap(Map map) throws SQLException {
		wrappedConnection.setTypeMap(map);
	}

	public Connection getWrappedConnection() {
		return wrappedConnection;
	}


	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		return wrappedConnection.createArrayOf(typeName, elements);
	}


	public Blob createBlob() throws SQLException {
		return wrappedConnection.createBlob();
	}


	public Clob createClob() throws SQLException {
		return wrappedConnection.createClob();
	}


	public NClob createNClob() throws SQLException {
		return wrappedConnection.createNClob();
	}


	public SQLXML createSQLXML() throws SQLException {
		return wrappedConnection.createSQLXML();
	}


	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		return wrappedConnection.createStruct(typeName, attributes);
	}


	public Properties getClientInfo() throws SQLException {
		return wrappedConnection.getClientInfo();
	}


	public String getClientInfo(String name) throws SQLException {
		return wrappedConnection.getClientInfo(name);
	}


	public boolean isValid(int timeout) throws SQLException {
		return wrappedConnection.isValid(timeout);
	}


	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return wrappedConnection.isWrapperFor(iface);
	}


	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		wrappedConnection.setClientInfo(properties);
	}


	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		wrappedConnection.setClientInfo(name, value);
	}


	public <T> T unwrap(Class<T> iface) throws SQLException {
		return wrappedConnection.unwrap(iface);
	}
	
	
}
