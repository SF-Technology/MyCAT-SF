package org.opencloudb.manager.parser.druid.statement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.opencloudb.manager.parser.druid.MycatASTVisitor;

import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * create mapfile 语句的解析结果
 * @author 01140003
 * @version 2017年4月12日 上午10:49:49 
 */
public class MycatCreateMapFileStatement extends MycatStatementImpl implements SQLDDLStatement {

    private static final Logger LOGGER = LoggerFactory.getLogger(MycatCreateMapFileStatement.class);

    private String fileName;
    private List<String> lines;

    @Override
    public void accept0(MycatASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public static MycatCreateMapFileStatement from(File mapFile) {
        MycatCreateMapFileStatement stmt = new MycatCreateMapFileStatement();
        List<String> fileContents = new ArrayList<>();
        stmt.setFileName(mapFile.getName());
        try {
            fileContents = Files.readLines(mapFile, Charsets.UTF_8);
        } catch (IOException e) {
            LOGGER.error("read file:{}/{} exception:{}!", mapFile.getPath(), mapFile.getName(), e.getMessage());
        }
        stmt.setLines(fileContents);
        return stmt;
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
