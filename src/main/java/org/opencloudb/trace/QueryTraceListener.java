package org.opencloudb.trace;

import org.opencloudb.stat.QueryResult;

public interface QueryTraceListener {
	
	public void onQueryResult(QueryResult queryResult);

}
