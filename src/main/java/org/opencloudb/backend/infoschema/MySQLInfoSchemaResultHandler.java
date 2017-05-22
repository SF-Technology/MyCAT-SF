package org.opencloudb.backend.infoschema;

import org.apache.log4j.Logger;
import org.opencloudb.mpp.PackWraper;
import org.opencloudb.sqlengine.*;
import java.util.List;

/**
 * Handler MySQL information_schema Results
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-09-08 14:42
 */


public class MySQLInfoSchemaResultHandler implements SQLJobHandler {
    public static final Logger LOGGER =
            Logger.getLogger(MySQLInfoSchemaResultHandler.class);
    private final MySQLInfoSchemaProcessor ctx;
    private List<byte[]> fields;

    public MySQLInfoSchemaResultHandler(MySQLInfoSchemaProcessor ctx){
        super();
        this.ctx = ctx;
    }

    @Override
    public void onHeader(String dataNode, byte[] header, List<byte[]> fields) {
        this.fields = fields;
    }

    @Override
    public boolean onRowData(String dataNode,byte[]rowData) {
        PackWraper packWraper = new PackWraper();
        packWraper.dataNode= dataNode;
        packWraper.rowData = rowData;
        ctx.addPack(packWraper);
        return false;
    }

    @Override
    public void finished(String dataNode, boolean failed) {
        ctx.endJobInput(dataNode,failed);
    }

}
