package org.opencloudb.manager.handler;

import org.opencloudb.backend.infoschema.MySQLInfoSchemaProcessor;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL查询结果处理链, 可以在一次查询收到结果并处理完结果以后再次进行下一次查询
 * @author CrazyPig
 * @since 2016-09-19
 *
 * @param <T>
 */
public abstract class SQLQueryResultHandlerChain<T> implements SQLQueryResultListener<T>  {
	private static final Logger LOGGER = LoggerFactory.getLogger(SQLQueryResultHandlerChain.class);

	protected FrontendConnection frontend;
	protected SQLQueryResultHandlerChain<T> next = null;
	protected String schema;
	protected String sql;
	
	public SQLQueryResultHandlerChain(FrontendConnection frontend, String schema, String sql) {
		this.frontend = frontend;
		this.schema = schema;
		this.sql = sql;
	}
	
	public void handle() throws Exception {
		MySQLInfoSchemaProcessor infoSchemaProcessor = new MySQLInfoSchemaProcessor(schema, sql, this);
		infoSchemaProcessor.processSQL();
	}
	
	public void setNextHandler(SQLQueryResultHandlerChain<T> next) {
		this.next = next;
	}
	
	public SQLQueryResultHandlerChain<T> next() {
		return next;
	}
	
	public void handleError(Throwable t) {
		if (this.frontend != null) {
			frontend.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, t.getMessage());
		}else {
			LOGGER.error(t.getMessage());
		}
	}
	
	public abstract void processResult(T result);
	
	@Override
	public void onResult(T result) {
		processResult(result);
		SQLQueryResultHandlerChain<T> next = next();
		if(next != null) {
			try {
				next.handle();
			} catch(Exception e) {
				handleError(e);
			}
		}
	}
	
}
