using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Justin.Database
{
    public class DataReaderRowCollection : IEnumerable<DataReaderRow>, IDisposable
    {
        private IDataReader m_reader;

        public DataReaderRowCollection(IDataReader reader)
        {
            m_reader = reader;
        }
        
        public void Dispose()
        {
            if(m_reader != null)
            {
                m_reader.Dispose();
            }
            GC.SuppressFinalize(this);
        }

        public IEnumerator<DataReaderRow> GetEnumerator()
        {
            var readerRow = DataReaderRow.NewInstance(m_reader);

            while (readerRow.Read())
            {
                yield return readerRow;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        
        public int FieldCount
        {
            get
            {
                return m_reader.FieldCount;
            }
        }
        public string GetFieldName(int index)
        {
            return m_reader.GetName(index);
        }
        public Type GetFieldType(int index)
        {
            return m_reader.GetFieldType(index);
        }
    }
}
