using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace SplitTableEngine.Jobs
{
    public class ArchiveTableJob : IJob
    {
        public void Process(string connectionString, string tableName)
        {
            string primaryKeyFieldName = SplitTableConfigHelper.GetConfigValue(connectionString, tableName, "PrimaryKeyFieldName");
            string createTableScript = SplitTableConfigHelper.GetConfigValue(connectionString, tableName, "ArchiveTableScript");
            string archiveWhereSql = SplitTableConfigHelper.GetConfigValue(connectionString, tableName, "ArchiveSQL");

            //把上个月、已完成的转入Archive表
            //把上个月、IsDeleted=1的转入Archive表
            DateTime yesterday=DateTime.Now.AddDays(-1);

            string contentArchiveTableName = string.Format("{0}.Archived.{1}", tableName, yesterday.ToString("yyyyMMdd"));
            createTableScript = string.Format(createTableScript, contentArchiveTableName);

            string createContentArchiveTableSql = @"
                                IF NOT EXISTS(
				                                SELECT	1
				                                FROM	[SplitTableMappings](NOLOCK)
				                                WHERE	[PhysicalTableName]='" + contentArchiveTableName + @"'
                                                  AND   [LogicalTableName]='" + tableName + @"'
			                                )
                                BEGIN
                                        "+ createTableScript + @"
                                END

                                IF NOT EXISTS(
				                                SELECT	1
				                                FROM	[SplitTableMappings](NOLOCK)
				                                WHERE	[PhysicalTableName]='" + contentArchiveTableName + @"'
                                                  AND   [LogicalTableName]='" + tableName + @"'
			                                )
                                BEGIN
                                        INSERT INTO [SplitTableMappings](LogicalTableName, PhysicalTableName) VALUES('"+tableName+@"', '" + contentArchiveTableName + @"')
                                END
";

            SqlUtil.ExecuteNonQuery(connectionString, createContentArchiveTableSql);

            string selectHotTableNamesSql = "SELECT [PhysicalTableName] FROM [SplitTableMappings](NOLOCK) WHERE HotTable=1 AND LogicalTableName='" + tableName + "'";

            List<string> hotTables = SqlUtil.GetStringList(connectionString, selectHotTableNamesSql);

            foreach (var hotTable in hotTables)
            {
                if (hotTable == contentArchiveTableName)
                    continue;

                string sql = @"
                                    BEGIN TRANSACTION

                                    BEGIN TRY
                                        DROP TABLE ##MoveRecords
                                    END TRY
                                    BEGIN CATCH
                                    END CATCH

                                    SELECT * INTO ##MoveRecords
                                    FROM
                                    (
                                        SELECT TOP 1000 * 
                                        FROM [" + hotTable + @"](READPAST)
                                        WHERE   " + archiveWhereSql + @"
                                    ) TBL

                                    DELETE FROM [" + hotTable + @"] WHERE [" + primaryKeyFieldName + @"] IN (SELECT [" + primaryKeyFieldName + @"] FROM ##MoveRecords(NOLOCK))
                                    
                                    INSERT INTO [" + contentArchiveTableName + @"] SELECT * FROM ##MoveRecords(NOLOCK)

                                    DROP TABLE ##MoveRecords

                                    COMMIT TRANSACTION
";
                SqlUtil.ExecuteNonQuery(connectionString, sql);
            }
        }
    }
}
