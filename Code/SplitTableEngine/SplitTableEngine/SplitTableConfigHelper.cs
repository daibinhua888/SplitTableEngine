using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SplitTableEngine
{
    internal static class SplitTableConfigHelper
    {
        internal static string GetConfigValue(string connectionString, string tableName, string configKey)
        {
            return SqlUtil.GetString(connectionString, string.Format("SELECT TOP 1 [ConfigValue] FROM [SplitTableConfigs](NOLOCK) WHERE [LogicalTableName]='{0}' AND [ConfigKey]='{1}' ", tableName, configKey));
        }
    }
}
