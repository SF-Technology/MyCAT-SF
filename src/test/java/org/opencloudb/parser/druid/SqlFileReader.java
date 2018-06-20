package org.opencloudb.parser.druid;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SqlFileReader {
	
	public static List<String> readAndGetSql(String sqlFile) {
		List<String> sqlList = new ArrayList<String>();
		try {
			BufferedReader bufReader = Files.newBufferedReader(Paths.get(SqlFileReader.class.getResource(sqlFile).toURI()), Charset.forName("UTF-8"));
			StringBuilder sb = new StringBuilder();
			String lineSep = System.getProperty("line.separator");
			String line = null;
			while((line = bufReader.readLine()) != null) {
				if(line.trim().isEmpty()) { // 空行, 忽略
					continue;
				}
				if(line.trim().startsWith("--")) { // 注释, 忽略
					continue;
				}
				if(sb.length() > 0) {
					sb.append(lineSep + line);
				} else {
					sb.append(line);
				}
				if(line.trim().endsWith(";")) {
					sqlList.add(sb.toString());
					sb = new StringBuilder();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return sqlList;
	}
	
	public static void main(String[] args) {
		
		List<String> sqlList = SqlFileReader.readAndGetSql("sqlFileReaderSample.txt");
		for(String sql : sqlList) {
			System.out.println("sql : " + sql);
		}
	}

}
