using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;
using System.Collections.Generic;

namespace Justin.Database.Test
{
    [TestClass]
    public class DatabaseTest
    {
        private Database GetDatabase()
        {
            return new OracleDatabase(@"Data Source=//www.tomtawsoft.com:2247/orcl;User ID=KTHIS5;Password=KingTHis");
        }

        [TestMethod]
        public void TestExcuteNonQuery()
        {
            using (var db = GetDatabase())
            {
                Assert.AreEqual(db.ExcuteNonQuery("insert into test (field) values (sysdate)"), 1);
            }
        }

        [TestMethod]
        public void TestQueryReader()
        {
            IEnumerable<Test> models = null;

            using (var db = GetDatabase())
            {
                models = db.QueryReader<Test>("select * from test",
                    (row) => new Test { Field = Convert.ToDateTime(row["field"]) });
                
                foreach (var m in models)
                {
                    Assert.IsTrue(db.IsOpen);
                }

                Assert.IsTrue(!db.IsOpen);
            }
        }

        [TestMethod]
        public void TestQuerySingle()
        {
            Test model = null;

            using (var db = GetDatabase())
            {
                model = db.QuerySingle<Test>("select * from test",
                    (row) => new Test { Field = Convert.ToDateTime(row["field"]) });

                Assert.IsTrue(!db.IsOpen);
            }

            Assert.IsNotNull(model);
        }

        [TestMethod]
        public void TestQuerySingleReaderRow()
        {
            DataReaderRow row = null;

            using (var db = GetDatabase())
            {
                row = db.QuerySingleReaderRow("select * from test");
                
                Assert.IsTrue(!db.IsOpen);
            }

            Assert.IsNotNull(row);
        }

        [TestMethod]
        public void TestQuerySingleDataRow()
        {
            using (var db = GetDatabase())
            {
                Assert.IsNotNull(db.QuerySingleDataRow("select * from test"));
                Assert.IsTrue(!db.IsOpen);
            }
        }

        [TestMethod]
        public void TestQueryScalar()
        {
            GetDatabase().QueryScalar<DateTime>("select * from test");
        }

        [TestMethod]
        public void TestQuery()
        {
            IEnumerable<Test> models = null;

            using (var db = GetDatabase())
            {
                models = db.Query<Test>("select * from test", 
                    (r) => new Test { Field = Convert.ToDateTime(r["Field"]) });

                Assert.IsTrue(!db.IsOpen);

                foreach (var m in models)
                {
                }
            }
        }

        [TestMethod]
        public void TestRollback()
        {
            var new_name = new Random().Next(1000000).ToString();

            var db = GetDatabase();
            Assert.IsTrue(!db.IsOpen);

            var old_name = db.QueryScalar<string>("select name from pub_emp where id=388");
            Assert.IsTrue(!db.IsOpen);

            db.BeginTransaction();
            Assert.IsTrue(db.IsOpen && db.HasTransaction);

            var i = db.ExcuteNonQuery($"update pub_emp set name ='{new_name}' where id=388");
            Assert.AreEqual(1, i);
            Assert.IsTrue(db.IsOpen && db.HasTransaction);

            db.RollbackTransaction();
            Assert.IsTrue(db.IsOpen && !db.HasTransaction);

            var name = db.QueryScalar<string>("select name from pub_emp where id=388");
            Assert.AreNotEqual(new_name, name);

            db.Close();
            Assert.IsTrue(!db.IsOpen);
        }

        [TestMethod]
        public void TestCommit()
        {
            var new_name = new Random().Next(1000000).ToString();

            var db = GetDatabase();
            Assert.IsTrue(!db.IsOpen);

            var old_name = db.QueryScalar<string>("select name from pub_emp where id=388");
            Assert.IsTrue(!db.IsOpen);

            db.BeginTransaction();
            Assert.IsTrue(db.IsOpen && db.HasTransaction);

            var i = db.ExcuteNonQuery($"update pub_emp set name ='{new_name}' where id=388");
            Assert.AreEqual(1, i);
            Assert.IsTrue(db.IsOpen && db.HasTransaction);

            db.CommitTransaction();
            Assert.IsTrue(db.IsOpen && !db.HasTransaction);

            var name = db.QueryScalar<string>("select name from pub_emp where id=388");
            Assert.AreEqual(new_name, name);

            db.Close();
            Assert.IsTrue(!db.IsOpen);
        }
    }

    class Test
    {
        public DateTime Field { get; set; }
    }
}
