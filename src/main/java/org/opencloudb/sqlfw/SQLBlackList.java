package org.opencloudb.sqlfw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * SQL Black List
 *
 * @author zagnix
 * @create 2016-10-24 9:13
 */

public class SQLBlackList implements H2DBInterface<SQLBlackList> {
    private final static Logger LOGGER = LoggerFactory.getLogger(SQLBlackList.class);

    private Integer id;
    private String originalSQL;
    private String modifiedSQL;

    public String getModifiedSQL() {
        return modifiedSQL;
    }

    public void setModifiedSQL(String modifiedSQL) {
        this.modifiedSQL = modifiedSQL;
    }

    public String getOriginalSQL() {
        return originalSQL;
    }

    public void setOriginalSQL(String originalSQL) {
        this.originalSQL = originalSQL;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SQLBlackList that = (SQLBlackList) o;

        if (originalSQL != null ? !originalSQL.equals(that.originalSQL) : that.originalSQL != null) return false;
        return modifiedSQL != null ? modifiedSQL.equals(that.modifiedSQL) : that.modifiedSQL == null;

    }

    @Override
    public int hashCode() {
        int result = originalSQL != null ? originalSQL.hashCode() : 0;
        result = 31 * result + (modifiedSQL != null ? modifiedSQL.hashCode() : 0);
        return result;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }


    @Override
    public void update() {

        /**
         * 1.查询是已经存在条sql
         */
        boolean isAdd = true;
        final Connection h2DBConn = H2DBManager.getH2DBManager().getH2DBConn();;
        Statement stmt = null;
        ResultSet rset = null;

        try {
            String sql = "select sql_id from sql_blacklist where sql_id = " +getId();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }

            stmt = h2DBConn.createStatement();
            rset = stmt.executeQuery(sql);
            if (rset.next()){
                isAdd = false;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
               try {
                   if(stmt !=null){
                    stmt.close();
                   }
                   if (rset !=null){
                       rset.close();
                   }
               } catch (SQLException e) {
                   e.printStackTrace();

           }
        }

        /**
         * 2.根据1决定是添加还是更新
         */

        String sql = null;

        if(isAdd){
            sql = "INSERT INTO sql_blacklist VALUES("+getId()+ ",'" + getOriginalSQL() + "')";
        }else {
            sql = "UPDATE sql_blacklist SET sql='" +getOriginalSQL() +"' WHERE sql_id = " + getId();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sql === >  " + sql);
        }

        try {
            stmt = h2DBConn.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                if(stmt !=null){
                    stmt.close();
                }
                if (rset !=null){
                    rset.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void update_row() {

    }

    @Override
    public void insert() {

    }

    @Override
    public SQLBlackList query(String key) {
        return null;
    }

    @Override
    public void delete() {
        final Connection h2DBConn = H2DBManager.getH2DBManager().getH2DBConn();;
        Statement stmt = null;
        ResultSet rset = null;

        try {

            String sql  = "DELETE FROM sql_blacklist WHERE sql_id = " + getId();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sql === >  " + sql);
            }

            stmt = h2DBConn.createStatement();
            stmt.execute(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {

            try {
                if(stmt !=null){
                    stmt.close();
                }

                if (rset !=null){
                    rset.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
