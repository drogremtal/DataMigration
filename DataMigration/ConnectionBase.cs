using SqlKata;
using System.Data;
using System.Data.Common;

namespace DataMigration
{
    internal interface ConnectionBase
    {
        void Connect();
        DataSet ExecuteDataSet(string sql);
        void ExecuteNonQuery(string Query, IEnumerable<DbParameter> dbParameters);

        SqlResult GenerateSql(Query query);
        DbParameter GetParameter(string key, object value);
    }
}