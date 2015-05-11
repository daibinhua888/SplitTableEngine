using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SplitTableEngine.Jobs
{
    internal interface IJob
    {
        void Process(string connectionString, string tableName);
    }
}
