using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Justin.Database
{
    public class OracleDatabase : Database
    {
        public OracleDatabase(string connectionString)
        {
            _connection = new OracleConnection(connectionString);
        }

        protected override DbDataAdapter CreateDataAdapter()
        {
            return new OracleDataAdapter();
        }
    }
}
