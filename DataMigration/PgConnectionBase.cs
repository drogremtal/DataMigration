using Npgsql;
using System.Data;

namespace DataMigration
{
    internal class ConnectionBase
    {
        string Connection = "Server=192.168.4.224;Port=5432;User ID='dbo';Password='123';Database=cms_portal;Pooling=true;Timeout=10;MinPoolSize=0;MaxPoolSize=100;ConnectionPruningInterval=10;";

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
    }
}