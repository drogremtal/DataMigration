using SqlKata;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMigration
{
    internal class Service
    {
        ConnectionBase ConnectionBase;

        public Service(ConnectionBase connectionBase)
        {

            ConnectionBase = connectionBase;
        }

        public List<String> GetAllTable(string sql)
        {

            List<string> res = new List<string>();

            var ds = ConnectionBase.ExecuteDataSet(sql);

            if (ds != null && ds.Tables.Count > 0)
            {
                var dt = ds.Tables[0];
                foreach (DataRow item in dt.Rows)
                {
                    res.Add(item[0].ToString().ToLowerInvariant());
                }
            }

            return res;
        }

        private static DbParameter[] GenerateParameters(Dictionary<string, object> bindings, IEnumerable<DbParameter> parameters)
        {
            var hasBindings = bindings != null && bindings.Count > 0;
            var hasParameters = parameters != null && parameters.Count() > 0;
            if (!hasBindings && !hasParameters)
            {
                return null;
            }

            var result = new List<DbParameter>();
            if (hasBindings)
            {
                foreach (var binding in bindings)
                {
                    var value = (binding.Value == null ? DBNull.Value : binding.Value);
                    // result.Add(GetDataProvider().GetParameter(binding.Key, value));
                }
            }
            if (hasParameters)
            {
                foreach (var parameter in parameters)
                {
                    //    result.Add(GetDataProvider().GetParameter(parameter.ParameterName, parameter.Value));
                }
            }
            return result.ToArray();
        }






        public static List<string> GetColumnsName(DataTable dt)
        {
            var res = new List<string>();

            foreach (DataColumn column in dt.Columns)
            {
                res.Add(column.ColumnName);
            }
            return res;
        }


        public static List<object[]> GetData(DataTable dt)
        {
            var res = new List<object[]> { };

            foreach (DataRow row in dt.Rows)
            {
                res.Add(row.ItemArray);
            }
            return res;
        }


        public void InsertData(Query query)
        {
            var sql = ConnectionBase.GenerateSql(query);

            var params = GenerateParameters(sql.NamedBindings);



        }


    }


}
