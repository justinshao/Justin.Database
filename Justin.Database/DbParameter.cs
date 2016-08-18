using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Justin.Database
{
    public class DbParameter
    {
        public DbParameter(string name, object value)
        {
            Name = name;
            Value = value;
        }

        public string Name { get; set; }
        public object Value { get; set; }
    }
}
