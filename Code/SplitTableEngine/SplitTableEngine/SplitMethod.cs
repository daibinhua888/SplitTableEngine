using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SplitTableEngine
{
    public struct SplitMethod
    {
        public SplitType SplitType { get; set; }
        public string Function_Call_Parameter { get; set; }
        public Func<object, string, string> Function_Call { get; set; }
    }
}
