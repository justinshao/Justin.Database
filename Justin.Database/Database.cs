using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Justin.Database
{
    public abstract class Database : IDisposable
    {
        /// <summary>
        /// 数据库连接对象
        /// </summary>
        protected DbConnection _connection;

        /// <summary>
        /// 和当前连接相关的事务对象
        /// </summary>
        private DbTransaction _transaction;

        #region 连接相关
        /// <summary>
        /// 打开数据库连接
        /// </summary>
        public void Open()
        {
            if (!IsOpen)
            {
                _connection.Open();
            }
        }

        /// <summary>
        /// 关闭数据库连接
        /// </summary>
        public void Close()
        {
            if (IsOpen)
            {
                _connection.Close();
            }
        }

        /// <summary>
        /// 连接是否打开
        /// </summary>
        public bool IsOpen
        {
            get
            {
                return _connection.State == System.Data.ConnectionState.Open;
            }
        }
        #endregion

        #region 事务相关
        /// <summary>
        /// 开启事务
        /// </summary>
        public void BeginTransaction()
        {
            if (!IsOpen)
            {
                Open();
            }

            _transaction = _connection.BeginTransaction();
        }

        /// <summary>
        /// 提交事务
        /// </summary>
        public void CommitTransaction()
        {
            if (HasTransaction)
            {
                _transaction.Commit();

                _transaction = null;
            }
        }

        /// <summary>
        /// 回滚事务
        /// </summary>
        public void RollbackTransaction()
        {
            if (HasTransaction)
            {
                _transaction.Rollback();

                _transaction = null;
            }
        }

        /// <summary>
        /// 判断当前的连接是否打开了事务
        /// </summary>
        public bool HasTransaction
        {
            get
            {
                return _transaction != null &&
                    IsOpen &&
                    _transaction.Connection == _connection;
            }
        }
        #endregion
        
        #region 增删改相关
        public int ExcuteNonQuery(string sql)
        {
            return ExcuteNonQuery(sql, new DbParameter[0]);
        }
        public int ExcuteNonQuery(string sql, params DbParameter[] parameters)
        {
            return ExcuteNonQuery(sql, (IEnumerable<DbParameter>)parameters);
        }
        /// <summary>
        /// 执行dml语句
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public int ExcuteNonQuery(string sql, IEnumerable<DbParameter> parameters)
        {
            bool hasTrans = HasTransaction;
            bool isOpen;

            if(!hasTrans)
            {
                isOpen = IsOpen;
            }
            else
            {
                isOpen = true;
            }

            if(!isOpen)
            {
                Open();
            }

            try
            {
                DbCommand cmd = _connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Transaction = _transaction;

                SetParameters(cmd, parameters);

                return cmd.ExecuteNonQuery();
            }
            finally
            {
                if (!hasTrans)
                {// 外面没有开事务，则默认执行后提交事务
                    CommitTransaction();
                }

                if (!isOpen)
                {// 如果外面没有打开连接，则默认执行后关闭连接
                    Close();
                }
            }
        }
        #endregion
        
        #region 查询相关
        public DataTable QueryTable(string sql)
        {
            return QueryTable(sql, new DbParameter[0]);
        }
        public DataTable QueryTable(string sql, params DbParameter[] parameters)
        {
            return QueryTable(sql, (IEnumerable<DbParameter>)parameters);
        }
        /// <summary>
        /// TataTable查询
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DataTable QueryTable(string sql, IEnumerable<DbParameter> parameters)
        {
            bool close = true;
            if (close = !IsOpen)
            {
                Open();
            }
            try
            {
                DbCommand cmd = _connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Transaction = _transaction;
                
                SetParameters(cmd, parameters);

                using (DbDataAdapter ad = CreateDataAdapter())
                {
                    DataTable dt = new DataTable();
                    ad.SelectCommand = cmd;
                    ad.Fill(dt);

                    return dt;
                }
            }
            finally
            {
                if (close)
                {
                    Close();
                }
            }
        }

        public IEnumerable<T> Query<T>(string sql, Func<DataRow, T> rowFun)
        {
            return Query<T>(sql, new DbParameter[0], rowFun);
        }
        public IEnumerable<T> Query<T>(string sql, Func<DataRow, T> rowFun, params DbParameter[] parameters)
        {
            return Query<T>(sql, parameters, rowFun);
        }
        /// <summary>
        /// 查询Model
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sql, IEnumerable<DbParameter> parameters, Func<DataRow, T> rowFun)
        {
            return new DataTableModels<T>(QueryTable(sql, parameters), rowFun);
        }

        public object QueryScalar(string sql)
        {
            return QueryScalar(sql, new DbParameter[0]);
        }
        public object QueryScalar(string sql, params DbParameter[] parameters)
        {
            return QueryScalar(sql, (IEnumerable<DbParameter>)parameters);
        }
        public object QueryScalar(string sql, IEnumerable<DbParameter> parameters)
        {
            return QueryScalar<object>(sql, parameters);
        }
        public T QueryScalar<T>(string sql)
        {
            return QueryScalar<T>(sql, new DbParameter[0]);
        }
        public T QueryScalar<T>(string sql, params DbParameter[] parameters)
        {
            return QueryScalar<T>(sql, (IEnumerable<DbParameter>)parameters);
        }
        /// <summary>
        /// 查询单个值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public T QueryScalar<T>(string sql, IEnumerable<DbParameter> parameters)
        {
            bool close = true;
            if(close = !IsOpen)
            {
                Open();
            }

            try
            {
                DbCommand cmd = _connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Transaction = _transaction;

                SetParameters(cmd, parameters);

                var obj = cmd.ExecuteScalar();

                return obj == null ? default(T) : (T)obj;
            }
            finally
            {
                if(close)
                {
                    Close();
                }
            }
        }

        public DataReaderRow QuerySingleReaderRow(string sql)
        {
            return QuerySingleReaderRow(sql, new DbParameter[0]);
        }
        public DataReaderRow QuerySingleReaderRow(string sql, params DbParameter[] parameters)
        {
            return QuerySingleReaderRow(sql, (IEnumerable<DbParameter>)parameters);
        }
        /// <summary>
        /// 查询单行（通过DataReader）
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DataReaderRow QuerySingleReaderRow(string sql, IEnumerable<DbParameter> parameters)
        {
            return QueryReader(sql, parameters).FirstOrDefault();
        }

        public DataRow QuerySingleDataRow(string sql)
        {
            return QuerySingleDataRow(sql, new DbParameter[0]);
        }
        public DataRow QuerySingleDataRow(string sql, params DbParameter[] parameters)
        {
            return QuerySingleDataRow(sql, (IEnumerable<DbParameter>)parameters);
        }
        /// <summary>
        /// 查询单行（通过DataTable）
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DataRow QuerySingleDataRow(string sql, IEnumerable<DbParameter> parameters)
        {
            return QueryTable(sql, parameters).AsEnumerable().FirstOrDefault();
        }

        public T QuerySingle<T>(string sql, Func<DataReaderRow, T> readerFun)
        {
            return QuerySingle(sql, new DbParameter[0], readerFun);
        }
        public T QuerySingle<T>(string sql, Func<DataReaderRow, T> readerFun, params DbParameter[] parameters)
        {
            return QuerySingle(sql, parameters, readerFun);
        }
        /// <summary>
        /// 查询单个Model
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string sql, IEnumerable<DbParameter> parameters, Func<DataReaderRow, T> readerFun)
        {
            return QueryReader(sql, parameters, readerFun).FirstOrDefault();
        }

        public IEnumerable<DataReaderRow> QueryReader(string sql)
        {
            return QueryReader(sql, new DbParameter[0]);
        }
        public IEnumerable<DataReaderRow> QueryReader(string sql, params DbParameter[] parameters)
        {
            return QueryReader(sql, (IEnumerable<DbParameter>)parameters);
        }
        public IEnumerable<DataReaderRow> QueryReader(string sql, IEnumerable<DbParameter> parameters)
        {
            return QueryReader<DataReaderRow>(sql, parameters, (row) => row);
        }
        public IEnumerable<T> QueryReader<T>(string sql, Func<DataReaderRow, T> readerFun)
        {
            return QueryReader<T>(sql, readerFun, new DbParameter[0]);
        }
        public IEnumerable<T> QueryReader<T>(string sql, Func<DataReaderRow, T> readerFun, params DbParameter[] parameters)
        {
            return QueryReader<T>(sql, parameters, readerFun);
        }
        /// <summary>
        /// 根据sql和参数以Reader的方式查询，最终返回一个IEnumerable的对象，以支持外界通过foreach的方式来使用
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <param name="readerFun">实现数据行到任何类型（往往是Model）的转换</param>
        /// <returns></returns>
        public IEnumerable<T> QueryReader<T>(string sql, IEnumerable<DbParameter> parameters, Func<DataReaderRow, T> readerFun)
        {
            bool close = true;
            if(close = !IsOpen)
            {
                Open();
            }

            try
            {

                DbCommand cmd = _connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Transaction = _transaction;

                SetParameters(cmd, parameters);

                using (IDataReader reader = cmd.ExecuteReader())
                {
                    DataReaderRow row = DataReaderRow.NewInstance(reader);
                    while (row.Read())
                    {
                        yield return readerFun(row);
                    }
                }
            }
            finally
            {
                if(close)
                {
                    Close();
                }
            }
        }
        #endregion

        protected abstract DbDataAdapter CreateDataAdapter();
        
        /// <summary>
        /// 销毁处理
        /// </summary>
        public void Dispose()
        {
            if(IsOpen)
            {
                Close();
            }
            _connection.Dispose();

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// 添加参数到数据库命令对象
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="parameters"></param>
        private static void SetParameters(DbCommand cmd, IEnumerable<DbParameter> parameters)
        {
            cmd.Parameters.Clear();

            foreach (var p in parameters)
            {
                var _p = cmd.CreateParameter();
                _p.ParameterName = p.Name;
                _p.Value = p.Value;

                cmd.Parameters.Add(_p);
            }
        }
    }

    class DataTableModels<T> : IEnumerable<T>
    {
        private DataTable _data;
        private Func<DataRow, T> _rowFun;

        public DataTableModels(DataTable data, Func<DataRow, T> rowFun)
        {
            _data = data;
            _rowFun = rowFun;
        }

        public IEnumerator<T> GetEnumerator()
        {
            foreach (var r in _data.AsEnumerable())
            {
                yield return _rowFun(r);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
