using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace LunarSoft.Library.Sql
{
    /// <summary>
    ///     Manage the connection to SQL Database.
    /// </summary>
    public abstract partial class SqlConnector
    {
        /*
         *  Properties
         */
        /// <summary>
        ///     Enable or disable mode using DBNull value when executing SQL command insert and update.
        ///     Default value of AllowDbNull is False.
        /// </summary>
        public bool AllowDbNull { get; set; } = false;

        protected ArrayList CommandParameters { get; } = new ArrayList();

        protected int CommandTimeout { get; set; } = 30;    // 30 seconds

        protected string ConnectionString { get; set; }


        /*
         *  Constructors
         */
        protected SqlConnector(string connectionString)
        {
            Validate(connectionString, MessageEnum.InvalidConnection);
            ConnectionString = connectionString;
        }

        protected SqlConnector(string connectionString, int commandTimeout) : this(connectionString)
        {
            CommandTimeout = commandTimeout;
        }


        /*
         *  Abstract Functions
         */
        protected abstract DataSet ExecuteMultipleQuery(string commandText, CommandType commandType);

        protected abstract TCollection ExecuteMultipleReader<T, TCollection>(
            string commandText, CommandType commandType)
            where T : class
            where TCollection : ICollection<T>;

        protected abstract int ExecuteNonQuery(string commandText, CommandType commandType);

        protected abstract DataTable ExecuteQuery(string commandText, CommandType commandType);

        protected abstract T ExecuteReader<T>(string commandText, CommandType commandType) where T : class;

        protected abstract string ExecuteScalar(string commandText, CommandType commandType);

        /// <summary>
        ///     Return true if connection is open successful, otherwise return false.
        /// </summary>
        /// <param name="message">The output message</param>
        public abstract bool TestConnection(out string message);


        /*
         *  Functions
         */
        /// <summary>
        ///     Executes the query from Store Procedure, and returns the number of rows affected.
        /// </summary>
        public int ExecuteProcedure(string procedureName)
        {
            return ExecuteNonQuery(procedureName, CommandType.StoredProcedure);
        }

        /// <summary>
        ///     Executes the query from Store Procedure, and returns the result set as DataSet.
        /// </summary>
        public void ExecuteProcedure(string procedureName, out DataSet dataSet)
        {
            dataSet = ExecuteMultipleQuery(procedureName, CommandType.StoredProcedure);
        }

        /// <summary>
        ///     Executes the query from Store Procedure, and returns the result set as DataTable.
        /// </summary>
        public void ExecuteProcedure(string procedureName, out DataTable dataTable)
        {
            dataTable = ExecuteQuery(procedureName, CommandType.StoredProcedure);
        }

        /// <summary>
        ///     Executes the query from Store Procedure, and returns the first column of the first row.
        /// </summary>
        public void ExecuteProcedure(string procedureName, out string value)
        {
            value = ExecuteScalar(procedureName, CommandType.StoredProcedure);
        }

        /// <summary>
        ///     Executes the query from Store Procedure, and returns the result set as Collection.
        /// </summary>
        public void ExecuteProcedure<T, TCollection>(string procedureName, out TCollection collection)
            where T : class
            where TCollection : ICollection<T>
        {
            collection = ExecuteMultipleReader<T, TCollection>(procedureName, CommandType.StoredProcedure);
        }

        /// <summary>
        ///     Executes the query from Store Procedure, and returns the result set as specify type.
        /// </summary>
        public void ExecuteProcedure<T>(string procedureName, out T dataObject) where T : class
        {
            dataObject = ExecuteReader<T>(procedureName, CommandType.StoredProcedure);
        }


        /// <summary>
        ///     Executes the query string, and returns the number of rows affected.
        /// </summary>
        public int ExecuteSql(string sql)
        {
            return ExecuteNonQuery(sql, CommandType.Text);
        }

        /// <summary>
        ///     Executes the query string, and returns the result set as DataSet.
        /// </summary>
        public void ExecuteSql(string sql, out DataSet dataSet)
        {
            dataSet = ExecuteMultipleQuery(sql, CommandType.Text);
        }

        /// <summary>
        ///     Executes the query string, and returns the result set as DataTable.
        /// </summary>
        public void ExecuteSql(string sql, out DataTable dataTable)
        {
            dataTable = ExecuteQuery(sql, CommandType.Text);
        }

        /// <summary>
        ///     Executes the query string, and returns the first column of the first row.
        /// </summary>
        public void ExecuteSql(string sql, out string value)
        {
            value = ExecuteScalar(sql, CommandType.Text);
        }

        /// <summary>
        ///     Executes the query string, and returns the result set as Collection.
        /// </summary>
        public void ExecuteSql<T, TCollection>(string sql, out TCollection collection)
            where T : class
            where TCollection : ICollection<T>
        {
            collection = ExecuteMultipleReader<T, TCollection>(sql, CommandType.Text);
        }

        /// <summary>
        ///     Executes the query string, and returns the result set as specify type.
        /// </summary>
        public void ExecuteSql<T>(string sql, out T dataObject) where T : class
        {
            dataObject = ExecuteReader<T>(sql, CommandType.Text);
        }


        protected static void Validate(string value, string exceptionMessage)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException(exceptionMessage);
            }
        }
    }
}