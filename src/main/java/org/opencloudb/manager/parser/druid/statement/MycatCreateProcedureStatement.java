package org.opencloudb.manager.parser.druid.statement;

import org.opencloudb.config.model.ProcedureConfig;
import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * 
 * @author 01169238
 * @since 1.5.3
 *
 */
public class MycatCreateProcedureStatement extends MycatStatementImpl implements SQLDDLStatement {
    
    private SQLName procedure;
    private SQLName schema;
    private SQLExpr dataNodes;
    
    @Override
    public void accept0(MycatASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
    
    public static MycatCreateProcedureStatement from(ProcedureConfig conf, String schema) {
        MycatCreateProcedureStatement stmt = new MycatCreateProcedureStatement();
        stmt.setProcedure(new SQLIdentifierExpr(conf.getName().toLowerCase()));
        stmt.setSchema(new SQLIdentifierExpr(schema));
        stmt.setDataNodes(new SQLCharExpr(conf.getDataNode()));
        return stmt;
    }

    public SQLName getProcedure() {
        return procedure;
    }

    public void setProcedure(SQLName procedure) {
        this.procedure = procedure;
    }

    public SQLExpr getDataNodes() {
        return dataNodes;
    }

    public void setDataNodes(SQLExpr dataNodes) {
        this.dataNodes = dataNodes;
    }

    public SQLName getSchema() {
        return schema;
    }

    public void setSchema(SQLName schema) {
        this.schema = schema;
    }
    
}
