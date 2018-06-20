package org.opencloudb.manager.parser.druid.statement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

/**
 * create mapfile 语句的解析结果
 * @author 01140003
 * @version 2017年4月12日 上午10:49:49 
 */
public class MycatCreateMapFileStatement extends MycatStatementImpl implements SQLDDLStatement {
	private String fileName;
	private List<String> lines;
	
	@Override
    public void accept0(MycatASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public static MycatCreateMapFileStatement from(File mapFile) {
	    MycatCreateMapFileStatement stmt = new MycatCreateMapFileStatement();
	    stmt.setFileName(mapFile.getName());
	    stmt.setLines(getLinesFromMapFile(mapFile));
	    return stmt;
	}
	
	private static List<String> getLinesFromMapFile(File mapFile) {
	    List<String> _lines = new ArrayList<String>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(mapFile)));
            String line = null;
            while ((line = reader.readLine()) != null) {
                _lines.add(line);
            }
        } catch (IOException e) {
            
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // escape
                }
            }
        }
        return _lines;
	}
	
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public List<String> getLines() {
		return lines;
	}
	public void setLines(List<String> lines) {
		this.lines = lines;
	}
}
