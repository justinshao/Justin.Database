using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Justin.Database
{
    public class MySqlDatabase : Database
    {
        public MySqlDatabase(string connectionString)
        {
            _connection = new MySqlConnection();
        }

        protected override DbDataAdapter CreateDataAdapter()
        {
            return new MySqlDataAdapter();
        }
    }
}
