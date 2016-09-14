package org.opencloudb.interceptor;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.parser.druid.DruidShardingParseInfo;
import org.opencloudb.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.druid.sql.visitor.SQLEvalVisitor.EVAL_VALUE_NULL;

/**
 * 实现SQL语句的拦截
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-09-13 15:45
 */

public final class SQLNewInterceptor {

    public static final Logger LOGGER = LoggerFactory.getLogger(SQLNewInterceptor.class);

    /**
     * 拦截SQL 成功返回true
     * @param schema
     * @param statement
     * @param ctx
     * @param sql
     * @param sc
     * @return
     */
    public static boolean interceptSQL(SchemaConfig schema, SQLStatement statement,
                                      DruidShardingParseInfo ctx, String sql,ServerConnection sc){

        Map<String, String> tableAliasMap = new HashMap<String, String>();
        Set<String> colSets = new LinkedHashSet<String>();
        Set<String> conditionColSets = new LinkedHashSet<String>();

        if(ctx !=null){
            tableAliasMap = ctx.getTableAliasMap();
        }


        MySqlSchemaStatVisitor mySQLKVisitor = new MySqlSchemaStatVisitor();
        statement.accept(mySQLKVisitor);

        Iterator<TableStat.Column> c = mySQLKVisitor.getColumns().iterator();

        while (c.hasNext()){
            TableStat.Column col = c.next();
            colSets.add(col.getName());
        }

        if (statement instanceof SQLInsertStatement
                || statement instanceof SQLUpdateStatement){
            String tName = mySQLKVisitor.getCurrentTable();

            if (tableAliasMap.size() > 0 && tableAliasMap.containsKey(tName)) {
                tName = tableAliasMap.get(tName);
            }

            if (schema != null) {
                    Map<String, TableConfig> map = schema.getTables();
                    TableConfig tableConfig = map.get(tName.toUpperCase());
                    if (tableConfig != null && !colSets.contains(tableConfig.getPartitionColumn().toLowerCase())) {
                        LOGGER.error("------->>>>> no sharding key!!!!!");
                    }else {
                        LOGGER.error("------->>>>>  sharding key!!!!!" + tableConfig.getPartitionColumn().toLowerCase() );
                    }
            }
        }


        List<TableStat.Condition> mergedConditionList = mySQLKVisitor.getConditions();

        ConcurrentHashMap<String,Map<String,String>> tableIndexMap =
                MycatServer.getInstance().getConfig().getTableIndexMap();

        /**
         * 找出ConditionList中所有的列名放在Set里面
         */
        for(TableStat.Condition condition : mergedConditionList) {
            TableStat.Column column = condition.getColumn();
            String colname = column.getName();
            if (tableAliasMap.size() > 0 && tableAliasMap.containsKey(colname)) {
                colname = tableAliasMap.get(colname);
            }
            conditionColSets.add(colname);
        }

        /**
         * 根据表名查找表定义的分片字段，是否出现在ConditionList中即分片字段
         */
        for(TableStat.Condition condition : mergedConditionList) {
            TableStat.Column column = condition.getColumn();
            String tableName = column.getTable();
            /**
             * 存在别名的情况，通过tableAliasMap取得真实的列名
             */
            if (tableAliasMap.size() > 0 && tableAliasMap.containsKey(tableName)) {
                tableName = tableAliasMap.get(tableName);
            }

            /**
             * 判断sql语句是否带分片字段
             */
            if (schema != null) {
                Map<String, TableConfig> map = schema.getTables();
                TableConfig tableConfig = map.get(tableName.toUpperCase());
                if (tableConfig != null &&!conditionColSets.contains(tableConfig.getPartitionColumn().toLowerCase())) {
                    LOGGER.error("------->>>>> no sharding key!!!!!");
                }else {
                    LOGGER.error("------->>>>>  sharding key!!!!!" + tableConfig.getPartitionColumn().toLowerCase() );
                }
            }
        }

        /**
         * SQL 拦截逻辑！！！
         * */
        for(TableStat.Condition condition : mergedConditionList) {
           TableStat.Column column = condition.getColumn();
           String tableName = column.getTable();
           String colname = column.getName();
           String operator = condition.getOperator();
           List<Object> values  = condition.getValues();

           /**
            * 存在别名的情况，通过tableAliasMap取得真实的列名
            */
           if(tableAliasMap.size() > 0 && tableAliasMap.containsKey(colname)){
               colname = tableAliasMap.get(colname);
           }

           if(tableAliasMap.size() > 0&& tableAliasMap.containsKey(tableName)){
               tableName = tableAliasMap.get(tableName);
           }

           LOGGER.error("tableName"+ tableName + "    colname  : " + colname);
           LOGGER.error("operator: " + operator.toString());
           LOGGER.error("values: " + values.toString());

           /**
            * 计算当前表在查询时影响最大行数
            */
           long currentDataRow = Long.MIN_VALUE;
           Map<String,String> indexMap = null;

           if (tableIndexMap.containsKey(tableName)) {
               indexMap =  tableIndexMap.get(tableName);
               long temp = 0L;
               for (String colkey:indexMap.keySet()) {
                   temp = Long.parseLong(indexMap.get(colkey).trim());
                   if (temp > currentDataRow){
                       currentDataRow = temp;
                   }
               }
               LOGGER.error("Current Data Volume :" + currentDataRow);
           }


           /**
            * 当前表的数据量小于10000，不做拦截。
            */
           if ((tableIndexMap.size()>0 && indexMap.size()>0
                   &&indexMap.containsKey(colname))
                   && currentDataRow <= 10000){
               return  false;
           }


           /**
            * step 0 .拦截掉select * from t 这样的语句
            */
           if (colSets.contains("*")){
               sc.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "" +
                       "The columns does not allow select * all table cols ...,SQL: " + sql);
               return true;
           }


           /**
            * step 1 .where 操作符带！=，<>,in,not in
            */
           if ((tableIndexMap.size()>0 && indexMap.size()>0
                   &&indexMap.containsKey(colname))
                   && (operator.equals("!=")
                   || operator.equals("<>")
                   || operator.equals("IN")
                   || operator.equals("NOT IN"))){
               sc.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "" +
                       "The index column does not allow the operator to be != or <> or IN or NOT IN ,SQL: " + sql);
               return true;
           }


           /**
            * step 2. where 后面字段是索引字段？
            */
           if (tableIndexMap.size()>0 && indexMap.size()>0
                   && !indexMap.containsKey(colname)){
               sc.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "" +
                       "The select of where conditional have not Index columns , SQL: " + sql);
               return true;
           }

           /**
            * step 3. where 后面字段是索引字段,但是值是NULL的情况
            */
           if (tableIndexMap.size()>0 && indexMap.size()>0
                   && indexMap.containsKey(colname)){
               Object value = values.get(0);
               if (value instanceof  Object && value == EVAL_VALUE_NULL){
                   sc.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "" +
                           "The index column does not allow  NULL  value , SQL : " + sql);
                   return true;
               }
           }

           /**
            * step 4. where 后面字段是索引字段,但是值是NULL的情况
            */
           if (tableIndexMap.size()>0 && indexMap.size()>0
                   && indexMap.containsKey(colname)
                   && operator.equals("LIKE")){
               Object value = values.get(0);
               if (value instanceof  String && ((String) value).startsWith("%")){
                   sc.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "" +
                           "The index column does not allow  like '%' value , SQL : " + sql);
                   return true;
               }
           }
       }
       return false;
   }
}
