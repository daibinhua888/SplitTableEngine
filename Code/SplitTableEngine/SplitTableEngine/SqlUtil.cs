using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace SplitTableEngine
{
    public static class SqlUtil
    {
        public static List<string> GetStringList(string connectionString, string sql)
        {
            var lst = new List<string>();
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                SqlCommand com = new SqlCommand(sql);

                com.Connection = con;

                con.Open();

                using (var reader = com.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        lst.Add(reader.GetString(0));
                    }
                }

                con.Close();
            }

            return lst;
        }

        public static string GetString(string connectionString, string sql)
        {
            var lst = new List<string>();
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                SqlCommand com = new SqlCommand(sql);

                com.Connection = con;

                con.Open();

                using (var reader = com.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        lst.Add(reader.GetString(0));
                    }
                }

                con.Close();
            }

            if (lst!=null&&lst.Count>0)
                return lst.First();

            return string.Empty;
        }

        public static void ExecuteNonQuery(string connectionString, string sql)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                SqlCommand com = new SqlCommand(sql);

                com.Connection = con;

                con.Open();

                com.ExecuteNonQuery();

                con.Close();
            }
        }
    }
}
