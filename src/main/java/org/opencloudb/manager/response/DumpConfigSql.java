package org.opencloudb.manager.response;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;

import com.google.common.base.Strings;

/**
 * dump sql的响应
 * @author 01169238
 * @since 1.5.3
 *
 */
public class DumpConfigSql {
    
    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("DUMP SQL", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        eof.packetId = ++packetId;
    }
    
    /**
     * 响应mycat_config dump 命令的结果, 
     * 如果contentToFile == null, 则将结果输出到客户端, 
     * 如果contentToFile不为空, 则将结果输出到本地文件
     * @param c
     * @param content
     * @param contentToFile
     */
    public static void response(ManagerConnection c, Appendable content, String contentToFile) {
        String rowContent = "";
        boolean dumpToFile = !Strings.isNullOrEmpty(contentToFile);
        if (dumpToFile) {
            // dump到本地文件
            try {
                // 将内容写到指定的本地文件路径
                dumpToLocalFile(contentToFile, content.toString(), c.getCharset());
                rowContent = "Dump to ${MYCAT_HOME}/conf/" + SystemConfig.getDumpFileFolder() + "/" + contentToFile;
                // 响应客户端dump到文件路径成功
                response0(c, rowContent);
            } catch (IOException e) {
                c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "Dump file meet IOException : " + e.getMessage());
                return ;
            } catch (URISyntaxException e) {
                c.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "Dump file meet exception : " + e.getMessage());
                return ;
            }
        } else {
            // 直接将dump出来的内容输出到客户端
            rowContent = content.toString();
            response0(c, rowContent);
        }
        

    }
    
    private static void response0(ManagerConnection c, String rowContent) {
        
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c, true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = eof.write(buffer, c, true);

        byte packetId = eof.packetId;

        // write row
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(rowContent, c.getCharset()));
        row.packetId = ++packetId;
        buffer = row.write(buffer, c, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }
    
    private static void dumpToLocalFile(String fileName, String content, String charset) throws UnsupportedEncodingException, IOException, URISyntaxException {
        Path basePath = Paths.get(SystemConfig.class.getClassLoader().getResource("").toURI());
        Path filePath = Paths.get(basePath.toString(), SystemConfig.getDumpFileFolder(), fileName);
        Files.write(filePath, content.getBytes(charset));
    }

}
