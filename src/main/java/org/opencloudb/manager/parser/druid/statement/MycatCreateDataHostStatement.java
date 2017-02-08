package org.opencloudb.manager.parser.druid.statement;

import java.util.ArrayList;
import java.util.List;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * 封装 create datahost 语句解析结果
 * @author CrazyPig
 * @since 2017-02-06
 *
 */
public class MycatCreateDataHostStatement extends MycatStatementImpl implements SQLDDLStatement {

	public static final SQLIntegerExpr DEFAULT_MAX_CON = new SQLIntegerExpr(1000);
	public static final SQLIntegerExpr DEFAULT_MIN_CON = new SQLIntegerExpr(5);
	public static final SQLIntegerExpr DEFAULT_BALANCE = new SQLIntegerExpr(0);
	public static final SQLExpr DEFAULT_DB_TYPE = new SQLCharExpr("mysql");
	public static final SQLExpr DEFAUTL_DB_DRIVER = new SQLCharExpr("native");
	public static final SQLIntegerExpr DEFAULT_SWITCH_TYPE = new SQLIntegerExpr(-1);
	
	private SQLName datahost;
	private SQLIntegerExpr maxCon = DEFAULT_MAX_CON;
	private SQLIntegerExpr minCon = DEFAULT_MIN_CON;
	private SQLExpr balance = DEFAULT_BALANCE;
	private SQLExpr mDbType = DEFAULT_DB_TYPE;
	private SQLExpr dbDriver = DEFAUTL_DB_DRIVER;
	private SQLIntegerExpr switchType = DEFAULT_SWITCH_TYPE;
	
	private List<Host> writeHosts = new ArrayList<Host>();
	
	public class Host {
		
		private SQLExpr host;
		private SQLExpr url;
		private SQLExpr user;
		private SQLExpr password;
		
		private List<Host> readHosts = new ArrayList<Host>();

		public SQLExpr getHost() {
			return host;
		}

		public void setHost(SQLExpr host) {
			this.host = host;
		}

		public SQLExpr getUrl() {
			return url;
		}

		public void setUrl(SQLExpr url) {
			this.url = url;
		}

		public SQLExpr getUser() {
			return user;
		}

		public void setUser(SQLExpr user) {
			this.user = user;
		}

		public SQLExpr getPassword() {
			return password;
		}

		public void setPassword(SQLExpr password) {
			this.password = password;
		}

		public List<Host> getReadHosts() {
			return readHosts;
		}

		public void setReadHosts(List<Host> readHosts) {
			this.readHosts = readHosts;
		}
		
	}
	
	@Override
	public void accept0(MycatASTVisitor visitor) {
		visitor.visit(this);
        visitor.endVisit(this);
	}

	public SQLName getDatahost() {
		return datahost;
	}

	public void setDatahost(SQLName datahost) {
		this.datahost = datahost;
	}
	

	public SQLIntegerExpr getMaxCon() {
		return maxCon;
	}

	public void setMaxCon(SQLIntegerExpr maxCon) {
		this.maxCon = maxCon;
	}

	public SQLIntegerExpr getMinCon() {
		return minCon;
	}

	public void setMinCon(SQLIntegerExpr minCon) {
		this.minCon = minCon;
	}

	public SQLExpr getBalance() {
		return balance;
	}

	public void setBalance(SQLExpr balance) {
		this.balance = balance;
	}

	public SQLExpr getmDbType() {
		return mDbType;
	}

	public void setmDbType(SQLExpr mDbType) {
		this.mDbType = mDbType;
	}

	public SQLExpr getDbDriver() {
		return dbDriver;
	}

	public void setDbDriver(SQLExpr dbDriver) {
		this.dbDriver = dbDriver;
	}

	public SQLIntegerExpr getSwitchType() {
		return switchType;
	}

	public void setSwitchType(SQLIntegerExpr switchType) {
		this.switchType = switchType;
	}

	public List<Host> getWriteHosts() {
		return writeHosts;
	}

	public void setWriteHosts(List<Host> writeHosts) {
		this.writeHosts = writeHosts;
	}

}
