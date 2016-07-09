namespace LunarSoft.Library.Sql
{
    public abstract partial class SqlConnector
    {
        protected internal static class MessageEnum
        {
            public const string ConnectionSuccess = "Connection Successful.";
            public const string InvalidConnection = "Connection String is invalid.";
            public const string InvalidDatabase = "Database Name is invalid.";
            public const string InvalidServer = "Server Name is invalid.";
            public const string InvalidUser = "User Name is invalid.";
        }
    }
}