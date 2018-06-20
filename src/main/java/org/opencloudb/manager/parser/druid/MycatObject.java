package org.opencloudb.manager.parser.druid;

import com.alibaba.druid.sql.ast.SQLObject;

public interface MycatObject extends SQLObject {

	void accept0(MycatASTVisitor visitor);

}
