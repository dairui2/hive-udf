package jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DDL {

	private static final String driver = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static final String url = "jdbc:hive://localhost:10000/log";
	private static final String uname = "";
	private static final String passwd = "";

	// 获取hive数据库的连接
	public Connection getCon() {
		Connection conn = null;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, uname, passwd);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	/** select:注意连接和计算超时的问题 **/
	public void selectTable(Connection conn) {

		String sql = "select domain,suv from log.autolog_1 limit 10";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next())
				System.out.println(rs.getString(1) + "\t" + rs.getString(2));

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	// 创建、删除、描述等
	public void createDrop(Connection conn) {
		String useSql = "use log";
		String createSql = "create table tt(name string)";
		String showSql = "show tables";
		String dropSql = "drop table tt";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			stmt.executeQuery(useSql);
			stmt.executeQuery(createSql);
			rs = stmt.executeQuery(showSql);
			while (rs.next())
				System.out.println(rs.getString(1));

			stmt.executeQuery(dropSql);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	// load数据;需要在hive客户端运行
	public void load(Connection conn) {

		String useSql = "use log";
		String loadSql = "load data local inpath 'D:/t.txt' overwrite into table tt";
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeQuery(useSql);
			stmt.executeQuery(loadSql);

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) {
		DDL ddl = new DDL();
		Connection conn = ddl.getCon();
		
		ddl.selectTable(conn);
		
		ddl.createDrop(conn);
		
		ddl.load(conn);

		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
