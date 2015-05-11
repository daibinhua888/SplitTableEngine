using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SplitTableEngine
{
    public static class TableConfigFactory
    {
        public static TableConfig CreateTableDescriptor(string connectionString, string tableName, 
                                                                    Func<object, string, string> splitFunction4ContentTable,
                                                                    Comparison<Dictionary<string, object>> ascendingSort,
                                                                    Comparison<Dictionary<string, object>> descendingSort)
        {
            TableConfig td = new TableConfig();

            td.ConnectionString = connectionString;

            td.PrimaryKeyFieldName = SplitTableConfigHelper.GetConfigValue(connectionString, tableName, "PrimaryKeyFieldName");
            td.TableName = tableName;
            td.ConnectionString = connectionString;
            td.SplitMethod = new SplitMethod()
            {
                SplitType = SplitType.Function,
                Function_Call = splitFunction4ContentTable,
                Function_Call_Parameter = string.Empty
            };
            td.AscendingSort = ascendingSort;
            td.DescendingSort = descendingSort;

            return td;
        }
    }
}
