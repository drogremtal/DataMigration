using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using SqlKata;
using SqlKata.Compilers;

namespace DataMigration
{
    internal class PgConnection : ConnectionBase
    {
        string Connection = "Server=192.168.4.224;Port=5432;User ID='dbo';Password='123';Database=cms_portal;Pooling=true;Timeout=10;MinPoolSize=0;MaxPoolSize=100;ConnectionPruningInterval=10;";

        public PgConnection(string connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }


        public void Connect()
        {
            DataSet ds = new DataSet();
            var conn = new NpgsqlConnection(Connection);
            conn.Open();
            using (var cmd = new NpgsqlCommand("Select 1"))
            {
                cmd.Connection = conn;
                var da = new NpgsqlDataAdapter(cmd);
                da.Fill(ds);
            }
        }

        public DataSet ExecuteDataSet(string sql)
        {
            DataSet ds = new DataSet();
            var conn = new NpgsqlConnection(Connection);
            conn.Open();
            using (var cmd = new NpgsqlCommand(sql))
            {
                cmd.Connection = conn;
                var da = new NpgsqlDataAdapter(cmd);
                da.Fill(ds);
            }
            return ds;
        }

        public void ExecuteNonQuery(string Query, IEnumerable<DbParameter> dbParameters)
        {

            using (var connection = new NpgsqlConnection(Connection))
            {
                connection.Open();

                using (var cmd  =  new NpgsqlCommand())
                {

                    cmd.Connection=connection;
                    cmd.CommandText = Query;
                    cmd.Parameters.AddRange(CastParameters(dbParameters));
                    cmd.ExecuteNonQuery();
                }

            }

        }

        public SqlResult GenerateSql(Query query)
        {
            var compiler = new PostgresCompiler();
            return compiler.Compile(query);
        }

        public DbParameter GetParameter(string key, object value)
        {
            return new NpgsqlParameter(key, value);
        }


        private DbParameter[] CastParameters(IEnumerable<DbParameter> parameters, bool isSp = false)
        {
            var result = new List<NpgsqlParameter>();
            if (parameters == null)
            {
                return result.ToArray();
            }
            foreach (var parameter in parameters)
            {
                var paramName = isSp ? "v_" + parameter.ParameterName.ToLower() : parameter.ParameterName.ToLower();
                result.Add(new NpgsqlParameter(paramName, parameter.Value ?? DBNull.Value) { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Unknown });
            }
            return result.ToArray();
        }
    }
}
