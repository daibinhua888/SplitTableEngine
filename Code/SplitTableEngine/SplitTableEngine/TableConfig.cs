using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SplitTableEngine
{
    public struct TableConfig
    {
        public SplitMethod SplitMethod { get; set; }
        public string TableName { get; set; }
        public string PrimaryKeyFieldName { get; set; }
        public string ConnectionString { get; set; }
        public Comparison<Dictionary<string, object>> AscendingSort { get; set; }
        public Comparison<Dictionary<string, object>> DescendingSort { get; set; }
    }
}
