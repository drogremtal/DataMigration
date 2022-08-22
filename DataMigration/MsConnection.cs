using Microsoft.Data.SqlClient;
using SqlKata.Compilers;
using SqlKata;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;

namespace DataMigration
{
    internal class MsConnection : ConnectionBase
    {

        string Connection;
        public MsConnection(string connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }


        public SqlResult GenerateSql(Query query)
        {
            var compiler = new SqlServerCompiler
            {
                UseLegacyPagination = true
            };
            return compiler.Compile(query);
        }


        public DbParameter GetParameter(string key, object value)
        {
            return new SqlParameter(key, value);
        }


        public void Connect()
        {
            using (SqlConnection connection = new SqlConnection(Connection))
            {
                connection.Open();
                Console.WriteLine("Подключение открыто");
                // Вывод информации о подключении
                Console.WriteLine("Свойства подключения:");
                Console.WriteLine($"\tСтрока подключения: {connection.ConnectionString}");
                Console.WriteLine($"\tБаза данных: {connection.Database}");
                Console.WriteLine($"\tСервер: {connection.DataSource}");
                Console.WriteLine($"\tВерсия сервера: {connection.ServerVersion}");
                Console.WriteLine($"\tСостояние: {connection.State}");
                Console.WriteLine($"\tWorkstationld: {connection.WorkstationId}");
            }
            Console.WriteLine("Подключение закрыто...");
        }


        public DataSet ExecuteDataSet(string sql)
        {
            // Создаем объект DataSet
            DataSet ds = new DataSet();

            using (SqlConnection connection = new SqlConnection(Connection))
            {
                // Создаем объект DataAdapter
                SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);

                // Заполняем Dataset
                adapter.Fill(ds);
            }
            return ds;
        }

        public void ExecuteNonQuery(string Query, IEnumerable<DbParameter> dbParameters)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection())
                {
                    SqlCommand sqlCommand = new SqlCommand();
                    sqlCommand.Connection = connection;
                    sqlCommand.Parameters.Clear();
                    sqlCommand.CommandText = Query;
                    sqlCommand.Parameters.AddRange(CastParameters(dbParameters));
                    connection.Open();
                    sqlCommand.ExecuteNonQuery();
                }
            }
            catch(Exception exc)  { 
            
                //Console.WriteLine(exc.ToString());
            }

        }

        private SqlParameter[] CastParameters(IEnumerable<DbParameter> parameters)
        {
            var result = new List<SqlParameter>();
            if (parameters == null)
            {
                return result.ToArray();
            }
            foreach (var parameter in parameters)
            {
                result.Add(new SqlParameter(parameter.ParameterName, parameter.Value ?? DBNull.Value));
            }
            return result.ToArray();
        }


    }

}
