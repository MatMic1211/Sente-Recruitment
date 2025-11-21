using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Isql;
using System.ComponentModel;
using System.Reflection.Metadata;
using System.Text;

namespace DbMetaTool
{

    public class DomainInfo
    {
        public required string Name { get; set; }
        public required string DataType { get; set; }
    }

    public class ColumnInfo
    {
        public required string Name { get; set; }
        public required string DataType { get; set; }
        public bool IsNullable { get; set; }
    }

    public class StoradeProcedureInfo
    {
        public required string Name { get; set; }
        public required string Source { get; set; }
    }

    public class StoradeProcedureParameterInfo
    {
        public required string Name { get; set; }
        public required string Direction { get; set; }
        public required string DataType { get; set; }
    }

    public static class Program
    {
        // Przykładowe wywołania:
        // DbMetaTool build-db --db-dir "C:\db\fb5" --scripts-dir "C:\scripts"
        // DbMetaTool export-scripts --connection-string "..." --output-dir "C:\out"
        // DbMetaTool update-db --connection-string "..." --scripts-dir "C:\scripts"
        public static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Użycie:");
                Console.WriteLine("  build-db --db-dir <ścieżka> --scripts-dir <ścieżka>");
                Console.WriteLine("  export-scripts --connection-string <connStr> --output-dir <ścieżka>");
                Console.WriteLine("  update-db --connection-string <connStr> --scripts-dir <ścieżka>");
                return 1;
            }

            try
            {
                var command = args[0].ToLowerInvariant();
                switch (command)
                {
                    case "build-db":
                        {
                            string dbDir = GetArgValue(args, "--db-dir");
                            string scriptsDir = GetArgValue(args, "--scripts-dir");

                            BuildDatabase(dbDir, scriptsDir);
                            Console.WriteLine("Baza danych została zbudowana pomyślnie.");
                            return 0;
                        }

                    case "export-scripts":
                        {
                            string connStr = GetArgValue(args, "--connection-string");
                            string outputDir = GetArgValue(args, "--output-dir");

                            ExportScripts(connStr, outputDir);
                            Console.WriteLine("Skrypty zostały wyeksportowane pomyślnie.");
                            return 0;
                        }

                    case "update-db":
                        {
                            string connStr = GetArgValue(args, "--connection-string");
                            string scriptsDir = GetArgValue(args, "--scripts-dir");

                            UpdateDatabase(connStr, scriptsDir);
                            Console.WriteLine("Baza danych została zaktualizowana pomyślnie.");
                            return 0;
                        }

                    default:
                        Console.WriteLine($"Nieznane polecenie: {command}");
                        return 1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd: " + ex.Message);
                return -1;
            }
        }

        private static string GetArgValue(string[] args, string name)
        {
            int idx = Array.IndexOf(args, name);
            if (idx == -1 || idx + 1 >= args.Length)
                throw new ArgumentException($"Brak wymaganego parametru {name}");
            return args[idx + 1];
        }

        /// <summary>
        /// Buduje nową bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void BuildDatabase(string databaseDirectory, string scriptsDirectory)
        {
            string server = "localhost";
            string user = "SYSDBA";
            string password = "masterkey";

            var csBuilder = new FbConnectionStringBuilder
            {
                DataSource = server,
                Database = databaseDirectory,
                UserID = user,
                Password = password,
                Charset = "UTF8",
                Dialect = 3
            };

            string connectionString = csBuilder.ToString();

            Console.WriteLine("Tworzenie bazy danych...");
            FbConnection.CreateDatabase(connectionString);
            Console.WriteLine($"Baza danych została utworzona: {databaseDirectory}");

            UpdateDatabase(connectionString, scriptsDirectory);
        }


        /// <summary>
        /// Generuje skrypty metadanych z istniejącej bazy danych Firebird 5.0.
        /// </summary>
        public static async void ExportScripts(string connectionString, string outputDirectory)
        {
            try
            {
                using var conn = new FbConnection(connectionString);
                await conn.OpenAsync();

                var sb = new StringBuilder();

                ExportDomains(conn, sb);
                ExportTables(conn, sb);
                ExportStoredProcedures(conn, sb);

                File.WriteAllText(Path.Combine(outputDirectory, "output.sql"), sb.ToString(), Encoding.UTF8);

                Console.WriteLine("Zakończono generowanie pliku output.sql");

            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd wykonywania skryptów: " + ex.Message);
            }
        }

        private static async void ExportTables(FbConnection conn, StringBuilder sb)
        {
            var tables = await GetTablesAsync(conn);

            foreach (var table in tables)
            {
                var columns = await GetColumnsAsync(conn, table);
                sb.AppendLine($"-- =======================================");
                sb.AppendLine($"-- TABLE: {table}");
                sb.AppendLine($"-- =======================================");
                sb.AppendLine($"CREATE TABLE {table} (");

                foreach (var col in columns)
                {
                    sb.AppendLine($"    {col.Name} {col.DataType}{(col.IsNullable ? "" : " NOT NULL")},");
                }

                sb.Remove(sb.Length - 3, 1);
                sb.AppendLine(");");
                sb.AppendLine();
            }
        }

        private static async void ExportDomains(FbConnection conn, StringBuilder sb)
        {
            var domains = await GetDomainsAsync(conn);

            foreach (var domain in domains)
            {
                sb.AppendLine($"-- =======================================");
                sb.AppendLine($"-- DOMAIN: {domain.Name}");
                sb.AppendLine($"-- =======================================");
                sb.AppendLine($"CREATE DOMAIN {domain.Name} AS \n{domain.DataType};");
                sb.AppendLine();
            }
        }

        private static async void ExportStoredProcedures(FbConnection conn, StringBuilder sb)
        {
            var storedProcedures = await GetStoredProdeduresAsync(conn);

            foreach (var sp in storedProcedures)
            {
                var parameters = await GetStoradeProcedureParameterAsync(conn, sp.Name);
                sb.AppendLine($"-- =======================================");
                sb.AppendLine($"-- STOREDPROCEDURE: {sp.Name}");
                sb.AppendLine($"-- =======================================");
                sb.AppendLine($"CREATE PROCEDURE {sp.Name}");
                if (parameters.Any(p => p.Direction == "1"))
                {
                    sb.AppendLine($"RETURNS (");
                    foreach (var parameter in parameters.Where(p => p.Direction == "1"))
                    {
                        sb.AppendLine($"    {parameter.Name} {parameter.DataType},");
                    }
                    sb.Remove(sb.Length - 3, 1);
                    sb.AppendLine(")");
                    sb.AppendLine("AS");
                    sb.AppendLine($"{sp.Source};");
                    sb.AppendLine();
                }
                sb.AppendLine();
            }
        }

        private static async Task<List<DomainInfo>> GetDomainsAsync(FbConnection conn)
        {
            var list = new List<DomainInfo>();

            string sql = @"
            SELECT 
            TRIM(RDB$FIELD_NAME) AS NAME,
            RDB$FIELD_LENGTH AS LENGTH,
            RDB$FIELD_TYPE AS FIELD_TYPE
            FROM RDB$FIELDS
            WHERE RDB$SYSTEM_FLAG = 0
              AND RDB$FIELD_NAME NOT LIKE 'RDB$%'
            ORDER BY RDB$FIELD_NAME;
            ";

            using var cmd = new FbCommand(sql, conn);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var domain = new DomainInfo
                {
                    Name = reader.GetString(0),
                    DataType = FirebirdTypeToSql(reader.GetInt32(2), reader.GetInt32(1))
                };

                list.Add(domain);
            }

            return list;
        }

        private static async Task<List<string>> GetTablesAsync(FbConnection conn)
        {
            var list = new List<string>();

            string sql = @"
                SELECT TRIM(RDB$RELATION_NAME)
                FROM RDB$RELATIONS
                WHERE RDB$SYSTEM_FLAG = 0
                AND RDB$VIEW_BLR IS NULL
            ";

            using var cmd = new FbCommand(sql, conn);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
                list.Add(reader.GetString(0));

            return list;
        }

        private static async Task<List<ColumnInfo>> GetColumnsAsync(FbConnection conn, string table)
        {
            var list = new List<ColumnInfo>();

            string sql = @"
            SELECT 
                TRIM(RF.RDB$FIELD_NAME) AS COLUMN_NAME,
                F.RDB$FIELD_LENGTH AS LENGTH,
                F.RDB$FIELD_TYPE AS TYPE,
                RF.RDB$NULL_FLAG AS NULL_FLAG
                FROM RDB$RELATION_FIELDS RF
                JOIN RDB$FIELDS F ON RF.RDB$FIELD_SOURCE = F.RDB$FIELD_NAME
                WHERE RF.RDB$RELATION_NAME = @TABLE
                ORDER BY RF.RDB$FIELD_POSITION
            ";

            using var cmd = new FbCommand(sql, conn);
            cmd.Parameters.AddWithValue("@TABLE", table);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var col = new ColumnInfo
                {
                    Name = reader.GetString(0),
                    DataType = FirebirdTypeToSql(reader.GetInt32(2), reader.GetInt32(1)),
                    IsNullable = reader.IsDBNull(3)
                };

                list.Add(col);
            }

            return list;
        }

        private static async Task<List<StoradeProcedureInfo>> GetStoredProdeduresAsync(FbConnection conn)
        {
            var list = new List<StoradeProcedureInfo>();
            var sql = @"
            SELECT 
            TRIM(RDB$PROCEDURE_NAME) AS NAME,
            TRIM(RDB$PROCEDURE_SOURCE) AS SRC
            FROM RDB$PROCEDURES
            ORDER BY RDB$PROCEDURE_NAME;
            ";

            using var cmd = new FbCommand(sql, conn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var name = reader["NAME"].ToString();
                var src = reader["SRC"]?.ToString();
                if (!string.IsNullOrEmpty(name) && !string.IsNullOrWhiteSpace(src))
                {
                    var sp = new StoradeProcedureInfo
                    {
                        Name = name,
                        Source = src
                    };
                    list.Add(sp);
                }
            }
            return list;
        }


        private static async Task<List<StoradeProcedureParameterInfo>> GetStoradeProcedureParameterAsync(FbConnection conn, string storadeProcedure)
        {
            var list = new List<StoradeProcedureParameterInfo>();

            string sql = @"
            SELECT
            TRIM(P.RDB$PARAMETER_NAME) AS PARAM_NAME,
            P.RDB$PARAMETER_TYPE AS PARAM_DIRECTION,
            F.RDB$FIELD_TYPE AS FIELD_TYPE,
            F.RDB$FIELD_LENGTH AS LENGTH
            FROM RDB$PROCEDURE_PARAMETERS P
            JOIN RDB$FIELDS F ON P.RDB$FIELD_SOURCE = F.RDB$FIELD_NAME
            WHERE P.RDB$PROCEDURE_NAME = @PROC
            ORDER BY P.RDB$PARAMETER_TYPE, P.RDB$PARAMETER_NUMBER;
            ";

            using var cmd = new FbCommand(sql, conn);
            cmd.Parameters.AddWithValue("@PROC", storadeProcedure);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var parameter = new StoradeProcedureParameterInfo
                {
                    Name = reader.GetString(0),
                    Direction = reader.GetString(1),
                    DataType = FirebirdTypeToSql(reader.GetInt32(2), reader.GetInt32(3)),
                };

                list.Add(parameter);
            }

            return list;
        }
        static string FirebirdTypeToSql(int type, int length)
        {
            return type switch
            {
                7 => "SMALLINT",
                8 => "INTEGER",
                10 => "FLOAT",
                12 => "DATE",
                13 => "TIME",
                14 => $"CHAR({length})",
                16 => "BIGINT",
                27 => "DOUBLE PRECISION",
                35 => "TIMESTAMP",
                37 => $"VARCHAR({length})",
                _ => "UNKNOWN"
            };
        }

        /// <summary>
        /// Aktualizuje istniejącą bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void UpdateDatabase(string connectionString, string scriptsDirectory)
        {
            if (!Directory.Exists(scriptsDirectory))
                throw new DirectoryNotFoundException($"Brak katalogu skryptów: {scriptsDirectory}");

            try
            {
                using var connection = new FbConnection(connectionString);
                connection.Open();
                Console.WriteLine("Połączenie z bazą danych zostało otwarte.");

                var sqlFiles = Directory.GetFiles(scriptsDirectory, "*.sql");
                Array.Sort(sqlFiles);

                foreach (var file in sqlFiles)
                {
                    Console.WriteLine($"Wykonuję: {Path.GetFileName(file)}");

                    string sqlText = File.ReadAllText(file);
                    var script = new FbScript(sqlText);
                    script.Parse();
                    var batch = new FbBatchExecution(connection);
                    foreach (var cmd in script.Results)
                    {
                        batch.Statements.Add(cmd);
                    }
                    batch.Execute();
                    Console.WriteLine($"Wykonano: {Path.GetFileName(file)}");
                }

                Console.WriteLine("Wszystkie skrypty zostały wykonane.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd wykonania skryptów: " + ex.Message);
            }
        }
    }
}
