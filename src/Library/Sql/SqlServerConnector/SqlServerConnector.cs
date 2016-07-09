using System;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;

namespace LunarSoft.Library.Sql
{
    /// <summary>
    ///     Manage the connection to SQL Server Database.
    /// </summary>
    public sealed class SqlServerConnector : SqlConnector
    {
        /*
         *  Properties
         */
        private const string ConnectionTemplate = "server={0};database={1};uid={2};pwd={3};";


        /*
         *  Constructors
         */
        public SqlServerConnector(string connectionString) : base(connectionString)
        {
        }

        public SqlServerConnector(string connectionString, int commandTimeout) : base(connectionString, commandTimeout)
        {
        }

        public SqlServerConnector(string serverName, string databaseName, string userName, string password)
            : base(BuildConnection(serverName, databaseName, userName, password))
        {
        }

        public SqlServerConnector(
            string serverName, string databaseName, string userName, string password, int commandTimeout)
            : base(BuildConnection(serverName, databaseName, userName, password), commandTimeout)
        {
        }


        /*
         *  Implement Functions
         */
        protected override DataSet ExecuteMultipleQuery(string commandText, CommandType commandType)
        {
            DataSet dsResult = new DataSet();
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                using (SqlCommand command = CreateCommand(connection, commandText, commandType))
                using (SqlDataAdapter dataAdapter = new SqlDataAdapter(command))
                {
                    dsResult.Locale = CultureInfo.InvariantCulture;
                    dataAdapter.Fill(dsResult);
                }
            }
            catch
            {
                dsResult.Dispose();
                throw;
            }
            return dsResult;
        }

        protected override TCollection ExecuteMultipleReader<T, TCollection>(string commandText, CommandType commandType)
        {
            TCollection collection = (TCollection) Activator.CreateInstance(typeof (TCollection));
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            using (SqlCommand command = CreateCommand(connection, commandText, commandType))
            {
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        collection.Add(Extract<T>(reader));
                    }
                }
            }
            return collection;
        }

        protected override int ExecuteNonQuery(string commandText, CommandType commandType)
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            using (SqlCommand command = CreateCommand(connection, commandText, commandType))
            {
                connection.Open();
                return command.ExecuteNonQuery();
            }
        }

        protected override DataTable ExecuteQuery(string commandText, CommandType commandType)
        {
            DataTable dtResult = new DataTable();
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                using (SqlCommand command = CreateCommand(connection, commandText, commandType))
                using (SqlDataAdapter dataAdapter = new SqlDataAdapter(command))
                {
                    dtResult.Locale = CultureInfo.InvariantCulture;
                    dataAdapter.Fill(dtResult);
                }
            }
            catch
            {
                dtResult.Dispose();
                throw;
            }
            return dtResult;
        }

        protected override T ExecuteReader<T>(string commandText, CommandType commandType)
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            using (SqlCommand command = CreateCommand(connection, commandText, commandType))
            {
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    return reader.Read() ? Extract<T>(reader) : null;
                }
            }
        }

        protected override string ExecuteScalar(string commandText, CommandType commandType)
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            using (SqlCommand command = CreateCommand(connection, commandText, commandType))
            {
                connection.Open();
                return command.ExecuteScalar()?.ToString();
            }
        }

        public override bool TestConnection(out string message)
        {
            return TestConnection(ConnectionString, out message);
        }


        /*
         *  Functions
         */
        /// <summary>
        ///     Allow adds query parameter for the prepare statement.
        /// </summary>
        public void AddParameter(string parameterName, SqlDbType dbType, object value)
        {
            CommandParameters.Add(new SqlParameter
            {
                Direction = ParameterDirection.Input,
                ParameterName = "@" + parameterName,
                SqlDbType = dbType,
                Value = value ?? GetDefaultValue(dbType)
            });
        }

        /// <summary>
        ///     Build and returns the connection string.
        /// </summary>
        public static string BuildConnection(
            string serverName, string databaseName, string userName, string password)
        {
            Validate(serverName, MessageEnum.InvalidServer);
            Validate(databaseName, MessageEnum.InvalidDatabase);
            Validate(userName, MessageEnum.InvalidUser);

            return string.Format(
                CultureInfo.InvariantCulture, ConnectionTemplate, serverName, databaseName, userName, password);
        }

        private SqlCommand CreateCommand(SqlConnection connection, string commandText, CommandType commandType)
        {
            SqlCommand command = connection.CreateCommand();
            command.CommandText = commandText;
            command.CommandType = commandType;
            command.CommandTimeout = CommandTimeout;
            command.Parameters.AddRange(CommandParameters.ToArray());

            // Clean up
            CommandParameters.Clear();

            return command;
        }

        private static T Extract<T>(IDataRecord reader) where T : class
        {
            Type type = typeof(T);
            T instance = (T)Activator.CreateInstance(type);
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.IsDBNull(i))
                {
                    continue;
                }
                type.GetField(reader.GetName(i))?.SetValue(instance, reader.GetString(i));
            }
            return instance;
        }

        private object GetDefaultValue(SqlDbType dbType)
        {
            if (AllowDbNull)
            {
                return DBNull.Value;
            }
            switch (dbType)
            {
                case SqlDbType.Bit:
                case SqlDbType.TinyInt:
                case SqlDbType.SmallInt:
                case SqlDbType.Int:
                case SqlDbType.BigInt:
                case SqlDbType.Float:
                case SqlDbType.Real:
                case SqlDbType.SmallMoney:
                case SqlDbType.Money:
                case SqlDbType.Decimal:
                    return 0;
                
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.VarChar:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                case SqlDbType.NText:
                    return string.Empty;

                default:
                    return DBNull.Value;
            }
        }

        /// <summary>
        ///     Allow tests the connection string.
        /// </summary>
        public static bool TestConnection(string connectionString, out string message)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    message = MessageEnum.ConnectionSuccess;
                    return true;
                }
            }
            catch (Exception exception)
            {
                message = exception.Message;
                return false;
            }
        }
    }
}