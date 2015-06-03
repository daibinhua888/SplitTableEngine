using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SplitTableEngine
{
    public static class Extentions
    {
        private static Dictionary<Type, PropertyInfo[]> typeProperties = new Dictionary<Type, PropertyInfo[]>();

        private static PropertyInfo[] GetGetProperties(Type type)
        {
            lock (typeProperties)
            {
                if(typeProperties.ContainsKey(type))
                    return typeProperties[type];

                PropertyInfo[] pi = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                typeProperties[type] = pi;

                return pi;
            }
        }

        public static Dictionary<string, object> ToDictionary(this object obj)
        {
            Dictionary<String, object> map = new Dictionary<string, object>();

            var pi = GetGetProperties(obj.GetType());

            foreach (PropertyInfo p in pi)
            {
                MethodInfo mi = p.GetGetMethod();

                if (mi != null && mi.IsPublic)
                {
                    map.Add(p.Name, mi.Invoke(obj, new Object[] { }));
                }
            }

            return map;  
        }

        public static Dictionary<string, object> ConvertSqlReaderToDictionary(this SqlDataReader sqlreader)
        {
            Dictionary<String, object> map = new Dictionary<string, object>();

            for(var i=0;i<sqlreader.FieldCount;i++)
            {
                string key = sqlreader.GetName(i);
                object value = sqlreader.GetValue(i);

                map.Add(key, value);
            }

            return map;
        }
    }
}
