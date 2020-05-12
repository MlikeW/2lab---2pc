using System;
using Npgsql;

namespace TwoPhaseCommitTransactions
{
    static class DbUtilities
    {
        public static NpgsqlConnection GetAndOpenConnection(string dbName)
        {
            var connection =
                new NpgsqlConnection(
                    $"Server=localhost; Port=5432; User Id=postgres; Password=110798; Database={dbName}");
            connection.Open();
            return connection;
        }

        public static T ExecuteCommandAndGetAnswer<T>(this NpgsqlConnection connection, string command,
            string column = default)
        {
            var cmd = new NpgsqlCommand(command, connection);
            NpgsqlDataReader dr = cmd.ExecuteReader();
            T variable = default;
            if (!string.IsNullOrEmpty(column))
            {
                while(dr.Read())
                {
                    variable = (T) dr[column];
                }
            }

            dr.Close();
            return variable;
        }

        public static void InsertIntoTable(this NpgsqlConnection connection, string tableWithRows,
            string values)
            => connection.ExecuteCommandAndGetAnswer<string>($"INSERT INTO {tableWithRows} VALUES {values}");

        public static void UpdateTable(this NpgsqlConnection connection, string table,
            string columnsAndValues, string condition)
            => connection.ExecuteCommandAndGetAnswer<string>($"UPDATE {table} SET {columnsAndValues} WHERE {condition}");

        public static T SelectFromTable<T>(this NpgsqlConnection connection, string table, string condition, string column = default,
            string rows = "*")
            => connection.ExecuteCommandAndGetAnswer<T>($"SELECT {rows} FROM {table} WHERE {condition}", column);

        public static T SelectAllFromTable<T>(this NpgsqlConnection connection, string table, string column = default)
            => connection.ExecuteCommandAndGetAnswer<T>($"SELECT * FROM {table}", column);

        public static void PrepareTransaction(this NpgsqlConnection connection, string name)
            => connection.ExecuteCommandAndGetAnswer<string>($"PREPARE TRANSACTION '{name}'");

        public static void CommitPrepared(this NpgsqlConnection connection, string name)
            => connection.ExecuteCommandAndGetAnswer<string>($"COMMIT PREPARED '{name}'");

        public static void RollbackPrepared(this NpgsqlConnection connection, string name)
            => connection.ExecuteCommandAndGetAnswer<string>($"ROLLBACK PREPARED '{name}'");

        public static string ToDate(this string date) => $"TO_DATE('{date}', 'YYYY-MM-DD')";

        public static string Minus(this string availabillity, int i = 1) => $"{availabillity} = {availabillity} - {i}";

        public static void CreateFlyTable(this NpgsqlConnection connection)
            => connection.ExecuteCommandAndGetAnswer<string>("CREATE TABLE FlyTable(" +
                                                             "FromCity varchar(255)," +
                                                             "ToCity varchar(255)," +
                                                             "DateTicket date," +
                                                             "price int," +
                                                             "availabillity int CHECK(availabillity >= 0)" +
                                                             ");");

        public static void CreateHotelTable(this NpgsqlConnection connection)
            => connection.ExecuteCommandAndGetAnswer<string>("CREATE TABLE LondonCoolHotel(" +
                                                             "roomnum int primary key," +
                                                             "roomprice int," +
                                                             "roomfrom date," +
                                                             "roomto date," +
                                                             "availabillity int CHECK(availabillity >= 0)" +
                                                             ");");

        public static void DropTableIfExist(this NpgsqlConnection connection, string table)
        {
            try
            {
                connection.ExecuteCommandAndGetAnswer<string>($"DROP TABLE {table}");
            }
            catch
            {
                Console.Write($"There was no table '{table}'");
            }
        }
    }
}