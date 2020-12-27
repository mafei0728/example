/*
    @Classname GetConnFromDurid
    @Description
    @author mafei0728
    @Date 2020/9/20 16:54
    @version 1.0
*/
package com.mafei.rdbs;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class GetConnFromDurid {
    private static DataSource dataSource;

    static {
        Properties properties = new Properties();
        InputStream ips = ClassLoader.getSystemClassLoader().getResourceAsStream("druid.properties");
        try {
            properties.load(ips);
            dataSource = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConn() {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException throwable) {
            throwable.printStackTrace();
        }
        return conn;
    }

    public static DataSource getDs() {
        return dataSource;
    }

    public static void main(String[] args) {
        Connection conn = GetConnFromDurid.getConn();
        PreparedStatement prm = null;
        ResultSet res = null;
        try {
            prm = conn.prepareStatement("select count(*) from user");
            res = prm.executeQuery();
            if (res.next()) {
                long num = res.getLong(1);
                System.out.println(num);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
