using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace SplitTableEngine
{
    internal static class SplitTableConfig
    {
        private const int _defaultSqlTimeout = 60;
        public static int SqlTimeout
        {
            get
            {
                object o = ConfigurationManager.AppSettings["SplitTable.SqlTimeout"];
                if (o == null)
                    return _defaultSqlTimeout;

                var s = Convert.ToString(o);

                if (string.IsNullOrEmpty(s))
                    return _defaultSqlTimeout;

                return Convert.ToInt32(s);
            }
        }
    }
}
