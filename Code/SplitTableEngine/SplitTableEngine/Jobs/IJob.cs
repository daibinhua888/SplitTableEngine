using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SplitTableEngine.Jobs
{
    public interface IJob
    {
        void Process(string connectionString, string tableName);
    }
}
