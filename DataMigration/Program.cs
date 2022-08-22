// See https://aka.ms/new-console-template for more information
using DataMigration;
using SqlKata;

Console.WriteLine("Hello, World!");

string PGConnection = "Server=192.168.4.224;Port=5432;User ID='dbo';Password='123';Database=cms_portal;Pooling=true;Timeout=10;MinPoolSize=0;MaxPoolSize=100;ConnectionPruningInterval=10;";


//string MsConnection = "Server=ksdb16\\sql2017;Database=cms_portal;User Id=admin;Password='';Timeout=100;";
string MsConnection = "server = ksdb16\\sql2017;uid='admin';pwd='';database=cms_portal;Pooling=true;Connect Timeout=10; Connection Lifetime=600;Encrypt=False";




#region     Подключение к базам

var ms = new MsConnection(MsConnection);
ms.Connect();

var pg = new PgConnection(PGConnection);
pg.Connect();

#endregion



///Поулчем список таблиц для MsSQL
var msservice = new Service(ms);
var AlltablesMs = msservice.GetAllTable("SELECT TABLE_NAME AS [table_name]   FROM INFORMATION_SCHEMA.TABLES WHERE table_type='BASE TABLE'");



///Поулчем список таблиц для pg
var pgsevice = new Service(pg);
var AlltablesPg = pgsevice.GetAllTable("SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_schema IN('dbo', 'myschema');");



foreach (var table in AlltablesMs)
{

    if (AlltablesPg.Contains(table))
    {
        var ds = ms.ExecuteDataSet($"Select * from {table}");
        if (ds != null && ds.Tables.Count > 0)
        {
            var dt = ds.Tables[0];
            if (dt != null && dt.Rows.Count > 0)
            {
                var _dt = dt;
                var columns = Service.GetColumnsName(dt);
                var data = Service.GetData(dt);

                var query = new Query($"{table}").AsInsert(columns,data);
                pgsevice.
                
            }

        }
    }
}







Console.ReadKey();


