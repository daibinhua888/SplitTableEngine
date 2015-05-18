using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace SplitTableEngine
{
    public class TableOperator
    {
        private TableConfig config;

        public TableOperator(TableConfig config)
        {
            this.config = config;
        }

        public Dictionary<string, object> SelectSingleInTable(string tableName, string pkId)
        {
            using (SqlConnection con = new SqlConnection(this.config.ConnectionString))
            {
                string sql = string.Format("SELECT TOP 1 * FROM [{0}](NOLOCK) WHERE {1}=@{1}", tableName, this.config.PrimaryKeyFieldName);
                SqlCommand com = new SqlCommand(sql);

                SqlParameter p = new SqlParameter(this.config.PrimaryKeyFieldName, pkId);
                com.Parameters.Add(p);

                com.Connection = con;

                con.Open();

                using (var reader = com.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.ConvertSqlReaderToDictionary();
                }

                con.Close();
            }

            return null;
        }

        public List<Dictionary<string, object>> SelectTopNInTable(string tableName, int maxCount, string whereSql, string orderBySql)
        {
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();

            using (SqlConnection con = new SqlConnection(this.config.ConnectionString))
            {
                string sql = string.Format("SELECT TOP {0} * FROM [{1}](NOLOCK) WHERE {2} ORDER BY {3}", 
                                                        maxCount,
                                                        tableName,
                                                        whereSql,
                                                        orderBySql);
                SqlCommand com = new SqlCommand(sql);

                com.Connection = con;

                con.Open();

                using (var reader = com.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader.ConvertSqlReaderToDictionary());
                    }
                }

                con.Close();
            }

            return result;
        }

        public int SelectCountInTable(string tableName, string whereSql)
        {
            int count = 0;

            using (SqlConnection con = new SqlConnection(this.config.ConnectionString))
            {
                string sql = string.Format("SELECT COUNT(1) FROM [{0}](NOLOCK) WHERE {1}",
                                                        tableName,
                                                        whereSql);
                SqlCommand com = new SqlCommand(sql);

                com.Connection = con;

                con.Open();

                count=Convert.ToInt32(com.ExecuteScalar());

                con.Close();
            }

            return count;
        }

        public bool UpdateInTable(string tableName, Dictionary<string, object> dictionary)
        {
            int rows = 0;

            string pkId = Convert.ToString(dictionary[this.config.PrimaryKeyFieldName]);

            using (SqlConnection con = new SqlConnection(this.config.ConnectionString))
            {
                //生成Sql语句
                //生成Sql parameter
                StringBuilder setSql = new StringBuilder();
                List<SqlParameter> parameters = new List<SqlParameter>();

                SqlParameter p;
                foreach (var item in dictionary)
                {
                    //过滤主键
                    if (item.Key.Equals(this.config.PrimaryKeyFieldName, StringComparison.OrdinalIgnoreCase))
                        continue;

                    setSql.Append(string.Format("[{0}]=@{0},", item.Key));

                    p = new SqlParameter(item.Key, item.Value);
                    parameters.Add(p);
                }

                p = new SqlParameter(this.config.PrimaryKeyFieldName, pkId);
                parameters.Add(p);

                string sql = string.Format("UPDATE [{0}] SET {1} WHERE {2}=@{2}", tableName, setSql.ToString().TrimEnd(",".ToCharArray()), this.config.PrimaryKeyFieldName);
                SqlCommand com = new SqlCommand(sql);
                com.Parameters.AddRange(parameters.ToArray());
                com.Connection = con;

                con.Open();

                rows = com.ExecuteNonQuery();

                con.Close();
            }

            return rows>0;
        }

        public void Insert(string mainTableName, Dictionary<string, object> dictionary)
        {
            string pkId = Convert.ToString(dictionary[this.config.PrimaryKeyFieldName]);

            using (SqlConnection con = new SqlConnection(this.config.ConnectionString))
            {
                //生成Sql语句
                //生成Sql parameter
                StringBuilder fieldSql = new StringBuilder();
                StringBuilder valueSql = new StringBuilder();
                List<SqlParameter> parameters = new List<SqlParameter>();

                foreach(var item in dictionary)
                {
                    fieldSql.Append(string.Format("[{0}],", item.Key));
                    valueSql.Append(string.Format("@{0},", item.Key));

                    SqlParameter p = new SqlParameter(string.Format("@{0}", item.Key), item.Value);
                    parameters.Add(p);
                }

                string sql = string.Format("INSERT INTO [{0}]({1}) VALUES({2})", mainTableName, fieldSql.ToString().TrimEnd(",".ToCharArray()), valueSql.ToString().TrimEnd(",".ToCharArray()));
                SqlCommand com = new SqlCommand(sql);
                com.Parameters.AddRange(parameters.ToArray());
                com.Connection = con;

                con.Open();

                int rows = com.ExecuteNonQuery();

                con.Close();
            }
        }

        public string CalculateTableNameBySplitMethod(object entity)
        {
            if (this.config.SplitMethod.SplitType == SplitType.No)
            {
                return this.config.TableName;
            }
            else if (this.config.SplitMethod.SplitType == SplitType.Function)
            {
                string tag = this.config.SplitMethod.Function_Call(entity, this.config.SplitMethod.Function_Call_Parameter);
                return string.Format("{0}.{1}", this.config.TableName, tag);  
            }

            throw new Exception("无法计算表名");
        }

        public List<string> GetAllHotTableNames()
        {
            List<string> tableNames = new List<string>();

            using (SqlConnection con = new SqlConnection(this.config.ConnectionString))
            {
                string sql = string.Format("SELECT [PhysicalTableName] FROM [SplitTableMappings](NOLOCK) WHERE [HotTable]=1 AND [LogicalTableName]='{0}'", this.config.TableName);

                SqlCommand com = new SqlCommand(sql);

                com.Connection = con;

                con.Open();

                using (var reader = com.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tableName = reader.GetString(0);

                        tableNames.Add(tableName);
                    }
                }

                con.Close();
            }

            return tableNames;
        }

        public List<string> GetAllArchiveTableNames()
        {
            List<string> tableNames = new List<string>();

            using (SqlConnection con = new SqlConnection(this.config.ConnectionString))
            {
                string sql = string.Format("SELECT [PhysicalTableName] FROM [SplitTableMappings](NOLOCK) WHERE ISNULL(HotTable, 0)=0  AND [LogicalTableName]='{0}'", this.config.TableName);

                SqlCommand com = new SqlCommand(sql);

                com.Connection = con;

                con.Open();

                using (var reader = com.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tableName = reader.GetString(0);

                        tableNames.Add(tableName);
                    }
                }

                con.Close();
            }

            return tableNames;
        }
    }
}
