using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Justin.Database
{
    class SqlDatabase : Database
    {
        public SqlDatabase(string connectionString)
        {
            _connection = new SqlConnection(connectionString);
        }

        protected override DbDataAdapter CreateDataAdapter()
        {
            return new SqlDataAdapter();
        }
    }
}
