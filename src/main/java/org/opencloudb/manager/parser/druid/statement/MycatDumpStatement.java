package org.opencloudb.manager.parser.druid.statement;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;

/**
 * 
 * @author 01169238
 * @since 1.5.3
 *
 */
public class MycatDumpStatement extends MycatStatementImpl {
    
    private MycatDumpStatementTarget target;
    /** 
     * 当target为ALL_SCHEMAS或者ALL_TABLES时, items应该为空, 其他情况, items里面填充的是具体的schema或者table
     */
    private List<SQLExpr> items = new ArrayList<SQLExpr>();
    
    /**
     * 是否dump内容到文件中(注意是保留到mycat程序运行的本地磁盘上, 默认放到/conf/dumpFile/下)
     */
    private SQLCharExpr intoFile;
    private boolean dumpIntoFile = false;
    
    public MycatDumpStatementTarget getTarget() {
        return target;
    }

    public void setTarget(MycatDumpStatementTarget target) {
        this.target = target;
    }
    
    public void addItem(SQLExpr item) {
        items.add(item);
    }
    
    public void addAllItems(List<SQLExpr> all) {
        items.addAll(all);
    }
    
    public List<SQLExpr> getItems() {
        return items;
    }

    public SQLCharExpr getIntoFile() {
        return intoFile;
    }

    public void setIntoFile(SQLCharExpr intoFile) {
        this.intoFile = intoFile;
    }

    public boolean isDumpIntoFile() {
        return dumpIntoFile;
    }

    public void setDumpIntoFile(boolean dumpIntoFile) {
        this.dumpIntoFile = dumpIntoFile;
    }
    
    
}
