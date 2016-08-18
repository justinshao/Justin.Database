using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Justin.Database
{
    public class DataReaderRow
    {
        private IDataReader _reader;
        
        internal static DataReaderRow NewInstance(IDataReader reader)
        {
            return new DataReaderRow
            {
                _reader = reader,
            };
        }
        
        internal bool Read()
        {
            return _reader.Read();
        }

        public object this[string column]
        {
            get
            {
                return _reader[column];
            }
        }
        public byte GetByte(int column)
        {
            return _reader.GetByte(column);
        }
        public short GetInt16(int column)
        {
            return _reader.GetInt16(column);
        }
        public int GetInt32(int column)
        {
            return _reader.GetInt32(column);
        }
        public long GetInt64(int column)
        {
            return _reader.GetInt64(column);
        }
        public char GetChar(int column)
        {
            return _reader.GetChar(column);
        }
        public decimal GetDecimal(int column)
        {
            return _reader.GetDecimal(column);
        }
        public double GetDouble(int column)
        {
            return _reader.GetDouble(column);
        }
        public float GetFloat(int column)
        {
            return _reader.GetFloat(column);
        }
        public bool GetBoolean(int column)
        {
            return _reader.GetBoolean(column);
        }
        public DateTime GetDateTime(int column)
        {
            return _reader.GetDateTime(column);
        }
        public Guid GetGuid(int column)
        {
            return _reader.GetGuid(column);
        }
        public string GetString(int column)
        {
            return _reader.GetString(column);
        }
        public T GetValue<T>(string column)
        {
            return (T)this[column];
        }

        public DataTable GetSchemaTable()
        {
            return _reader.GetSchemaTable();
        }
    }
}
