using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace SplitTableEngine
{
    public class SplitTable
    {
        private TableConfig config;
        private TableOperator tableHelper;

        public SplitTable(TableConfig config)
        {
            this.config = config;
            this.tableHelper = new TableOperator(this.config);
        }

        public Dictionary<string, object> FindByID(string pkId)
        {
            List<string> mainTableNames = this.tableHelper.GetAllHotTableNames();//拿到所有可能的表名

            foreach (var tableName in mainTableNames)
            {
                Dictionary<string, object> results = this.tableHelper.SelectSingleInTable(tableName, pkId);
                if (results != null && results.Count > 0)
                    return results;
            }

            List<string> archiveTableNames = this.tableHelper.GetAllArchiveTableNames();
            foreach (var tableName in archiveTableNames)
            {
                Dictionary<string, object> results = this.tableHelper.SelectSingleInTable(tableName, pkId);
                if (results != null && results.Count > 0)
                    return results;
            }

            return null;
        }

        public void Insert(object entity)
        {
            //定位tablename，从meta table中
            string mainTableName = this.tableHelper.CalculateTableNameBySplitMethod(entity);//根据算法得到表名

            this.tableHelper.Insert(mainTableName, entity.ToDictionary());
        }

        public bool Update(object entity)
        {
            string pkId = Convert.ToString(entity.ToDictionary()[this.config.PrimaryKeyFieldName]);

            List<string> mainTableNames = this.tableHelper.GetAllHotTableNames();//拿到所有可能的表名

            foreach (var tableName in mainTableNames)
            {
                Dictionary<string, object> results = this.tableHelper.SelectSingleInTable(tableName, pkId);
                if (results != null && results.Count > 0)
                {
                    //更新具体miantable中的记录
                    return this.tableHelper.UpdateInTable(tableName, entity.ToDictionary());
                }
            }

            return false;
        }

        public List<Dictionary<string, object>> SelectTopN(int maxCount, string whereSql, string orderBySql, bool withNoLockOption = true)
        {
            //获取所有的可能表名
            List<string> mainTableNames = this.tableHelper.GetAllHotTableNames();//拿到所有可能的表名（由于需要orderby，以后这里需要优化、拆分）

            List<Dictionary<string, object>> lst = new List<Dictionary<string, object>>();

            if (mainTableNames == null)
                return null;

            mainTableNames.ForEach(tableName =>
            {
                List<Dictionary<string, object>> tempResult = this.tableHelper.SelectTopNInTable(tableName, maxCount, whereSql, orderBySql, withNoLockOption);

                if(tempResult!=null)
                    lst.AddRange(tempResult);
            });

            //排序
            if (string.IsNullOrEmpty(orderBySql) || orderBySql.IndexOf("asc", StringComparison.OrdinalIgnoreCase) >= 0)
                lst.Sort(this.config.AscendingSort);
            else
                lst.Sort(this.config.DescendingSort);

            return lst.Take(maxCount).ToList();
        }

        public int SelectCount(string whereSql)
        {
            //获取所有的可能表名
            List<string> mainTableNames = this.tableHelper.GetAllHotTableNames();//拿到所有可能的表名（由于需要orderby，以后这里需要优化、拆分）

            int count = 0;

            if (mainTableNames == null)
                return count;

            mainTableNames.ForEach(tableName =>
            {
                int tempResult = this.tableHelper.SelectCountInTable(tableName, whereSql);

                count += tempResult;
            });

            return count;
        }
    }
}
