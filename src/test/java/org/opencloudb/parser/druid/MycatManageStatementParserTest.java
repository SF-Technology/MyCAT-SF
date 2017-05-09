package org.opencloudb.parser.druid;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.opencloudb.manager.parser.druid.MycatManageStatementParser;
import org.opencloudb.manager.parser.druid.statement.MycatListStatementTarget;
import org.opencloudb.manager.parser.druid.statement.MycatAlterUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCheckTbStructConsistencyStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateChildTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatCreateUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataHostStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropDataNodeStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropSchemaStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropTableStatement;
import org.opencloudb.manager.parser.druid.statement.MycatDropUserStatement;
import org.opencloudb.manager.parser.druid.statement.MycatListStatement;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import org.opencloudb.manager.parser.druid.statement.MycatChecksumTableStatement;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.parser.ParserException;

import junit.framework.Assert;

public class MycatManageStatementParserTest {
	
	@Test(expected = ParserException.class)
	public void testParseCheckTbStructConsistencyStatement() {
		String sql = "check table structure consistency for TESTDB.*, SCHEMA1.TB1";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		Assert.assertEquals(MycatCheckTbStructConsistencyStatement.class, stmt.getClass());
		Assert.assertEquals(2, ((MycatCheckTbStructConsistencyStatement)stmt).getNameList().size());
		Assert.assertEquals("TESTDB.*", ((MycatCheckTbStructConsistencyStatement)stmt).getNameList().get(0).toString());
		Assert.assertEquals("SCHEMA1.TB1", ((MycatCheckTbStructConsistencyStatement)stmt).getNameList().get(1).toString());

		// TODO where条件解析
		sql = "check table structure consistency for TESTDB where table_name = ?";
		parser = new MycatManageStatementParser(sql);
		stmt = parser.parseStatement();
		MycatCheckTbStructConsistencyStatement _stmt = (MycatCheckTbStructConsistencyStatement) stmt;
		Assert.assertEquals("table_name = ?", _stmt.getWhere().toString());
		
		// 解析抛错
		sql = "check table structure consistency";
		parser = new MycatManageStatementParser(sql);
		stmt = parser.parseStatement();
	}
	
	@Test
	public void testParseCreateSchemaStatementSuccess() {
		String sql = "create schema TESTDB dataNode = 'dn1' checkSQLschema = true sqlMaxLimit = 10000;";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		Assert.assertEquals(MycatCreateSchemaStatement.class, stmt.getClass());
		MycatCreateSchemaStatement _stmt = (MycatCreateSchemaStatement) stmt;
		Assert.assertEquals("TESTDB", _stmt.getSchema().getSimpleName().toUpperCase());
		Assert.assertEquals(10000, _stmt.getSqlMaxLimit());
		Assert.assertEquals(true, _stmt.isCheckSQLSchema());
		assertEquals("dn1", _stmt.getDataNode());
		sql = "create schema TESTDB dataNode = 'dn1' sqlMaxLimit = 10000 checkSQLschema = false;";
		parser = new MycatManageStatementParser(sql); 
		stmt = parser.parseStatement();
		sql = "create schema TESTDB dataNode = 'dn1';";
		parser = new MycatManageStatementParser(sql);
		stmt = parser.parseStatement();
	}
	
	@Test
	public void testParseCreateSchemaStatementFail() {
		
		testFailFromFile("create_schema_fail_test.txt");
		
	}
	
	@Test
	public void testParseCreateTableStatementSuccess() {
		StringBuilder sb = new StringBuilder();
		sb.append("create table tb1 in TESTDB")
			.append(" global = false")
			.append(" autoIncrement = false")
			.append(" primaryKey = \"ID\"")
			.append(" dataNode = \"dn1,dn2,dn3\"")
			.append(" rule = \"mod3\"");
		String sql = sb.toString();
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		Assert.assertEquals(MycatCreateTableStatement.class, stmt.getClass());
		
		testSuccessFromFile("create_table_success_test.txt");
		
	}
	
	@Test
	public void testParseCreateTableStatementFail() {
		testFailFromFile("create_table_fail_test.txt");
	}
	
	@Test
	public void testParseCreateChildTableStatementSuccess() {
		StringBuilder sb = new StringBuilder();
		sb.append("create childtable chtb1 in TESTDB")
			.append(" parent = \"tb1\"")
			.append(" parentKey = \"id\"")
			.append(" autoIncrement = true")
			.append(" joinKey = \"pid\"")
			.append(" primaryKey = \"id\"");
		String sql = sb.toString();
		MycatManageStatementParser parser= new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		Assert.assertEquals(MycatCreateChildTableStatement.class, stmt.getClass());
		MycatCreateChildTableStatement _stmt = (MycatCreateChildTableStatement) stmt;
		Assert.assertEquals("chtb1", _stmt.getTable().getSimpleName());
		Assert.assertEquals("TESTDB", _stmt.getSchema().getSimpleName());
		Assert.assertEquals("tb1", ((SQLCharExpr)_stmt.getParentTable()).getValue().toString());
		Assert.assertEquals("id", ((SQLCharExpr)_stmt.getParentKey()).getValue().toString());
		Assert.assertEquals("pid", ((SQLCharExpr)_stmt.getJoinKey()).getText());
		Assert.assertEquals("id", ((SQLCharExpr)_stmt.getPrimaryKey()).getText());
		
		testSuccessFromFile("create_childtable_success_test.txt");
		
	}
	
	@Test
	public void testParseCreateChildTableStatementFail() {
		testFailFromFile("create_childtable_fail_test.txt");
	}
	
	@Test
	public void testParseCreateDataNodeStatementSuccess() {
		
		String sql = "create datanode dn1 datahost = 'dh1' database = 'db1'";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		Assert.assertEquals(MycatCreateDataNodeStatement.class, stmt.getClass());
		MycatCreateDataNodeStatement _stmt = (MycatCreateDataNodeStatement) stmt;
		Assert.assertEquals("dn1", _stmt.getDatanode().getSimpleName());
		Assert.assertEquals("dh1", ((SQLCharExpr)_stmt.getDatahost()).getText());
		Assert.assertEquals("db1", ((SQLCharExpr)_stmt.getDatabase()).getText());
		
	}
	
	@Test
	public void testParseCreateDataNodeStatementFail() {
		
		testFailFromFile("create_datanode_fail_test.txt");
		
	}
	
	@Test
	public void testParseCreateDataHostStatementSuccess() {
		
		StringBuilder sb = new StringBuilder();
		sb.append("create datahost dh1")
			.append(" maxCon = 500")
			.append(" minCon = 5")
			.append(" balance = 1")
			.append(" dbType = \"mysql\"")
			.append(" dbDriver = \"native\"")
			.append(" switchType = -1")
			.append(" WITH writeHosts ({")
			.append(" host = \"w1\"")
			.append(" url = \"localhost:3306\"")
			.append(" user = \"root\"")
			.append(" password = \"mysql\"")
			.append("})");
		String sql = sb.toString();
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		Assert.assertEquals(MycatCreateDataHostStatement.class, stmt.getClass());
		
		testSuccessFromFile("create_dh_success_test.txt");
	}
	
	@Test
	public void testParseCreateDataHostStatementFail() {
		
		testFailFromFile("create_dh_fail_test.txt");
		
	}
	
	@Test
	public void testParseCreateUserStatementSuccess() {
		
		StringBuilder sb = new StringBuilder();
		sb.append("create user usr01")
			.append(" password = 'sf123456'")
			.append(" schemas = 'testdb, cjx'")
			.append(" readOnly = true");
		String sql = sb.toString();
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		assertEquals(MycatCreateUserStatement.class, stmt.getClass());
		MycatCreateUserStatement _stmt = (MycatCreateUserStatement) stmt;
		assertEquals("usr01", _stmt.getUserName().getSimpleName());
		assertEquals("sf123456", ((SQLCharExpr)_stmt.getPassword()).getText());
		assertEquals("testdb, cjx", ((SQLCharExpr)_stmt.getSchemas()).getText());
		assertEquals(true, _stmt.isReadOnly());
		
		testSuccessFromFile("create_user_success_test.txt");
		
		
	}
	
	@Test
	public void testParseCreateUserStatementFail() {
		
		testFailFromFile("create_user_fail_test.txt");
		
	}
	
	@Test(expected = ParserException.class)
	public void testParseDropSchemaStatement() {
		String sql = "drop schema TESTDB";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		Assert.assertEquals(MycatDropSchemaStatement.class, stmt.getClass());
		Assert.assertEquals(sql.toUpperCase(), stmt.toString());
		
		sql = "drop schema TESTDB sdbfds";
		parser = new MycatManageStatementParser(sql);
		parser.parseStatement();
	}
	
	@Test(expected = ParserException.class)
	public void testParseDropTableStatement() {
		String sql = "drop table tb1";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		assertEquals(MycatDropTableStatement.class, stmt.getClass());
		MycatDropTableStatement _stmt = (MycatDropTableStatement) stmt;
		assertEquals("tb1", _stmt.getTable().getSimpleName());
		
		sql = "drop table tb1 dfdsfd";
		parser = new MycatManageStatementParser(sql);
		parser.parseStatement();
	}
	
	@Test(expected = ParserException.class)
	public void testParseDropDataNodeStatement() {
		String sql = "drop datanode dn1";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		assertEquals(MycatDropDataNodeStatement.class, stmt.getClass());
		MycatDropDataNodeStatement _stmt = (MycatDropDataNodeStatement) stmt;
		assertEquals("dn1", _stmt.getDataNode().getSimpleName());
		
		sql = "drop datanode dn1 dd";
		parser = new MycatManageStatementParser(sql);
		parser.parseStatement();
	}
	
	@Test(expected = ParserException.class)
	public void testParseDropDataHostStatement() {
		String sql = "drop datahost dh1";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		assertEquals(MycatDropDataHostStatement.class, stmt.getClass());
		MycatDropDataHostStatement _stmt = (MycatDropDataHostStatement) stmt;
		assertEquals("dh1", _stmt.getDataHost().getSimpleName());
		
		sql = "drop datahost dh1 dd";
		parser = new MycatManageStatementParser(sql);
		parser.parseStatement();
	}
	
	@Test
	public void testParseDropUserStatement() {
		String sql = "drop user usr01";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		assertEquals(MycatDropUserStatement.class, stmt.getClass());
		MycatDropUserStatement _stmt = (MycatDropUserStatement) stmt;
		assertEquals("usr01", _stmt.getUserName().getSimpleName());
	}
	
	@Test
	public void testParseAlterUserStatement() {
		String sql = "alter user user01 password = 'newpasswd' schemas = 'db1, db2, db3'";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		assertEquals(MycatAlterUserStatement.class, stmt.getClass());
		MycatAlterUserStatement _stmt = (MycatAlterUserStatement) stmt;
		assertEquals("user01", _stmt.getUserName().getSimpleName());
		assertEquals(true, _stmt.isAlterPassword());
		assertEquals(true, _stmt.isAlterSchemas());
		assertEquals("newpasswd", ((SQLCharExpr)_stmt.getPassword()).getText());
		assertEquals("db1, db2, db3", ((SQLCharExpr)_stmt.getSchemas()).getText());
	}
	
	@Test
	public void testParseListStatement() {
		String sql = "list schemas";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		assertEquals(MycatListStatement.class, stmt.getClass());
		assertEquals(MycatListStatementTarget.SCHEMAS, ((MycatListStatement)stmt).getTarget());
		
		sql = "list tables";
		parser = new MycatManageStatementParser(sql);
		stmt = parser.parseStatement();
		assertEquals(MycatListStatementTarget.TABLES, ((MycatListStatement)stmt).getTarget());
		
		sql = "list datanodes";
		parser = new MycatManageStatementParser(sql);
		stmt = parser.parseStatement();
		assertEquals(MycatListStatementTarget.DATANODES, ((MycatListStatement)stmt).getTarget());
		
		sql = "list datahosts";
		parser = new MycatManageStatementParser(sql);
		stmt = parser.parseStatement();
		assertEquals(MycatListStatementTarget.DATAHOSTS, ((MycatListStatement)stmt).getTarget());
		
		sql = "list rules";
		parser = new MycatManageStatementParser(sql);
		stmt = parser.parseStatement();
		assertEquals(MycatListStatementTarget.RULES, ((MycatListStatement)stmt).getTarget());
		
		sql = "list functions";
		parser = new MycatManageStatementParser(sql);
		stmt = parser.parseStatement();
		assertEquals(MycatListStatementTarget.FUNCTIONS, ((MycatListStatement)stmt).getTarget());
		
		sql = "list users";
		parser = new MycatManageStatementParser(sql);
		stmt = parser.parseStatement();
		assertEquals(MycatListStatementTarget.USERS, ((MycatListStatement)stmt).getTarget());
		
//		sql = "list unknown";
//		parser = new MycatManageStatementParser(sql);
//		parser.parseStatement();
	}
	
	@Test(expected = ParserException.class)
	public void testParseListStatementFail() {
		String sql = "list unknown";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		parser.parseStatement();
	}
	
	private void testSuccessFromFile(String sqlFile) {
		List<String> sqlList = SqlFileReader.readAndGetSql(sqlFile);
		for(String sql1 : sqlList) {
			MycatManageStatementParser parser = new MycatManageStatementParser(sql1);
			parser.parseStatement();
		}
	}
	
	private void testFailFromFile(String sqlFile) {
		testFailFromFile(sqlFile, false);
	}
	
	private void testFailFromFile(String sqlFile, boolean printErrorStackTrace) {
		List<String> sqlList = SqlFileReader.readAndGetSql(sqlFile);
		for(String sql : sqlList) {
			boolean failOccur = false;
			try {
				MycatManageStatementParser parser = new MycatManageStatementParser(sql);
				parser.parseStatement();
			} catch(ParserException e) {
				if(printErrorStackTrace) {
					e.printStackTrace();
				}
				StringBuffer sb = new StringBuffer();
				String lineSep = System.getProperty("line.separator");
				sb.append("parse sql error => " + e.getMessage() + lineSep);
				sb.append("[sql] " + sql + lineSep);
				System.out.println(sb.toString());
				failOccur = true;
				Assert.assertEquals(true, failOccur);
			}
		}
	}
		
	@Test(expected = ParserException.class)
	public void testParseChecksumTableStatement() {
		String sql = "checksum table TESTDB.hotnews";
		MycatManageStatementParser parser = new MycatManageStatementParser(sql);
		SQLStatement stmt = parser.parseStatement();
		Assert.assertEquals(MycatChecksumTableStatement.class, stmt.getClass());
		MycatChecksumTableStatement _stmt = (MycatChecksumTableStatement)stmt;
		Assert.assertEquals("TESTDB.hotnews", _stmt.getTableName().toString());
		SQLPropertyExpr tableName = (SQLPropertyExpr) _stmt.getTableName();
		Assert.assertEquals("TESTDB", tableName.getOwner().toString());
		Assert.assertEquals("hotnews", tableName.getSimpleName());
	
		sql = "checksum TESTDB.hotnews";
		parser = new MycatManageStatementParser(sql);
		stmt = parser.parseStatement();
	}
	
}
