using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SplitTableEngine
{
    /// <summary>
    /// 分表方式
    /// </summary>
    public enum SplitType
    {
        /// <summary>
        /// 不分表
        /// </summary>
        No,

        /// <summary>
        /// 分区函数
        /// </summary>
        Function
    }
}
