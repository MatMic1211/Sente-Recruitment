using FirebirdSql.Data.FirebirdClient;
using System;
using System.IO;

namespace DbMetaTool
{
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
            if (!Directory.Exists(databaseDirectory))
                Directory.CreateDirectory(databaseDirectory);

            string databasePath = Path.Combine(databaseDirectory, "database.fdb");

            string server = "localhost";
            string user = "SYSDBA";
            string password = "masterkey";

            var csBuilder = new FbConnectionStringBuilder
            {
                DataSource = server,
                Database = databasePath,
                UserID = user,
                Password = password,
                Charset = "UTF8",
                Dialect = 3
            };

            string connectionString = csBuilder.ToString();
            if (!File.Exists(databasePath))
            {
                Console.WriteLine("Tworzenie bazy danych...");
                FbConnection.CreateDatabase(connectionString);
                Console.WriteLine($"Baza danych utworzona: {databasePath}");
            }
            else
            {
                Console.WriteLine("Baza istnieje. Przechodzę dalej.");
            }

            try
            {
                using var connection = new FbConnection(connectionString);
                connection.Open();
                Console.WriteLine("Połączono z bazą danych.");

                var sqlFiles = Directory.GetFiles(scriptsDirectory, "*.sql");
                Array.Sort(sqlFiles);

                foreach (var file in sqlFiles)
                {
                    Console.WriteLine($"Wykonuję: {Path.GetFileName(file)}");

                    string script = File.ReadAllText(file);
                    using var command = new FbCommand(script, connection);
                    command.ExecuteNonQuery();

                    Console.WriteLine($"✓ Wykonano: {Path.GetFileName(file)}");
                }

                Console.WriteLine("Wszystkie skrypty wykonane.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd wykonywania skryptów: " + ex.Message);
            }
        }


        /// <summary>
        /// Generuje skrypty metadanych z istniejącej bazy danych Firebird 5.0.
        /// </summary>
        public static void ExportScripts(string connectionString, string outputDirectory)
        {
            // TODO:
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            // 2) Pobierz metadane domen, tabel (z kolumnami) i procedur.
            // 3) Wygeneruj pliki .sql / .json / .txt w outputDirectory.
            throw new NotImplementedException();
        }

        /// <summary>
        /// Aktualizuje istniejącą bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void UpdateDatabase(string connectionString, string scriptsDirectory)
        {
            // TODO:
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            // 2) Wykonaj skrypty z katalogu scriptsDirectory (tylko obsługiwane elementy).
            // 3) Zadbaj o poprawną kolejność i bezpieczeństwo zmian.
            throw new NotImplementedException();
        }
    }
}
