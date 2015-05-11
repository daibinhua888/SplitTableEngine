using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SplitTableEngine.Jobs
{
    public class MoveRecordsInTablesJob : IJob
    {
        public void Process(string connectionString, string tableName)
        {
            string primaryKeyFieldName = SplitTableConfigHelper.GetConfigValue(connectionString, tableName, "PrimaryKeyFieldName");

            string selectHotTableNamesSql = "SELECT [PhysicalTableName] FROM [SplitTableMappings] WHERE HotTable=1 AND LogicalTableName='"+tableName+"'";

            List<string> hotTables = SqlUtil.GetStringList(connectionString, selectHotTableNamesSql);

            foreach(var processingHotTable in hotTables)
            {
                foreach (var moveRecords2TableName in hotTables)
                {
                    if (moveRecords2TableName == processingHotTable)
                        continue;

                    string whereSql = SplitTableConfigHelper.GetConfigValue(connectionString, tableName, string.Format("SplitWhereSql.[{0}]", moveRecords2TableName));

                    string sql = @"
                                        BEGIN TRANSACTION

                                        SELECT * INTO ##MoveRecords
                                        FROM
                                        (
                                            SELECT * FROM [" + processingHotTable + @"] WHERE " + whereSql + @"
                                        ) TBL

                                        DELETE FROM [" + processingHotTable + @"] 
                                        WHERE [" + primaryKeyFieldName + @"] IN (
                                                                                    SELECT [" + primaryKeyFieldName + @"]
                                                                                    FROM    ##MoveRecords
                                                                                )

                                        INSERT INTO [" + moveRecords2TableName + @"] SELECT * FROM ##MoveRecords

                                        DROP TABLE ##MoveRecords

                                        COMMIT
";

                    SqlUtil.ExecuteNonQuery(connectionString, sql);
                }
            }
        }
    }
}
