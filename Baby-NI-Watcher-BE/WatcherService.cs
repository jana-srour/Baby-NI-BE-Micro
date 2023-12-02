using System.Security.Cryptography;
using Vertica.Data.Internal.DotNetDSI;
using Vertica.Data.VerticaClient;

namespace Baby_NI_Watcher_BE
{
    public class WatcherService
    {

        private readonly Microsoft.Extensions.Logging.ILogger _logger;

        private string watchFolder;
        private string verticaConnection;
        private static string totalFilePath;

        private Dictionary<string, string> fileChecksums = new Dictionary<string, string>();
        private HashSet<string> processedFiles = new HashSet<string>();

        private int counter = 2;

        private FileSystemWatcher watcher;

        public WatcherService() { }

        //Start watching the folder needed
        public WatcherService(Microsoft.Extensions.Logging.ILogger logger, string watchFolder, string verticaConnection)
        {

            _logger = logger;

            this.watchFolder = watchFolder;
            this.verticaConnection = verticaConnection;

            //Check if the folder does exists to watch
            if (!Directory.Exists(watchFolder))
            {
                Console.WriteLine(">> Folder not found.");
                _logger.LogError(">> Folder not found.");
                return;
            }

            // initialize the watcher
            watcher = new FileSystemWatcher();
            watcher.Path = watchFolder;
            watcher.Filter = "*.txt";
            watcher.EnableRaisingEvents = true;
            watcher.NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite | NotifyFilters.Size;

            // Handle the events
            watcher.Created += OnFileCreated;
            watcher.Renamed += OnFileRenamed;
            watcher.Deleted += OnFileDeleted;
            watcher.Changed += OnFileChanged;
            watcher.Error += OnError;
        }

        // Watch for creation events
        private void OnFileCreated(object sender, FileSystemEventArgs e)
        {
            string filePath = e.FullPath;
            string fileName = Path.GetFileName(filePath);

            bool stillCopying = IsFileStillBeingCopied(filePath);

            Console.WriteLine("###############################################################################################");
            _logger.LogInformation("###############################################################################################");
            //Check if file still copying if yes wait for it
            if (stillCopying)
            {
                Console.WriteLine($">> File is still being copied: {filePath}");
                WaitForFileCopyCompletion(filePath); // Wait for the file to be fully copied
            }

            //Check for validity
            if (IsFileValid(fileName, filePath))
            {
                // Check for duplicate files by their names
                using (VerticaConnection connection = new VerticaConnection(verticaConnection))
                {
                    connection.Open();

                    string[] headers = { "file_name", "isParsed", "isLoaded", "isReady" };
                    string[] dataTypes = { "VARCHAR(255)", "BOOLEAN", "BOOLEAN", "BOOLEAN" };

                    // Check if the table exists
                    if (!TableExists(connection, "FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION"))
                    {
                        Console.WriteLine(">> Table does not exist.");

                        string createTableSql = GenerateCreateTableSql("FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION", headers, dataTypes);

                        // Create the table in the database
                        using (VerticaCommand command = connection.CreateCommand())
                        {
                            command.CommandText = createTableSql;
                            command.ExecuteNonQuery();
                            Console.WriteLine(">>> Table created successfully.");
                            _logger.LogInformation(">>> Table created successfully.");
                        }
                    }

                    using (VerticaCommand command = connection.CreateCommand())
                    {
                        command.CommandText = $"SELECT * FROM FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION WHERE file_name='{fileName}'";

                        // if the file does exist in the table exit this method otherwise insert the file into the table for duplication prevention
                        using (VerticaDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                if (reader.Read())
                                {
                                    // Retrieving value of isParsed column of the file
                                    string file_name = reader.GetString(reader.GetOrdinal("file_name"));

                                    if (file_name == fileName)
                                    {
                                        Console.WriteLine($">> Duplicate file detected: {filePath}.");
                                        _logger.LogCritical($">> Duplicate file detected: {filePath}.");
                                        File.Delete(filePath);
                                    }
                                    else
                                    {
                                        Console.WriteLine($">> Valid file created: {filePath}");
                                        _logger.LogInformation($">> Valid file created: {filePath}");
                                        //ProcessFile(fileName, filePath);
                                    }

                                }
                            }
                            else
                            {
                                object[] data = new object[4];
                                string fileExecuted = Path.GetFileNameWithoutExtension(fileName);
                                bool isParsed = false;
                                bool isLoaded = false;
                                bool isReady = false;

                                data[0] = fileExecuted;
                                data[1] = isParsed;
                                data[2] = isLoaded;
                                data[3] = isReady;

                                string insertSql = GenerateInsertSql("FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION", headers, data);


                                using (VerticaCommand command2 = connection.CreateCommand())
                                {
                                    command2.CommandText = insertSql;
                                    command2.ExecuteNonQuery();
                                }
                            }


                        }

                    }
                }
            }
            else
            {
                Console.WriteLine($">> Invalid file: {filePath}");
            }

            totalFilePath = filePath;

        }

        // Generate the SQL Query for Table Creation 
        private string GenerateCreateTableSql(string tableName, string[] headers, string[] dataTypes)
        {
            var sql = $@"CREATE TABLE {tableName} (";

            for (int i = 0; i < headers.Length; i++)
            {
                // Append column definition
                sql += $"{headers[i]} {dataTypes[i]}";

                // Add comma if it's not the last column
                if (i < headers.Length - 1)
                {
                    sql += ", ";
                }
            }

            sql += @");";

            return sql;
        }

        // Generates inserto into query
        private string GenerateInsertSql(string tableName, string[] headers, object[] data)
        {
            var sql = $"INSERT INTO {tableName} (";
            sql += string.Join(", ", headers.Select(header => $"\"{header}\"")); // Enclose column names in double quotes
            sql += ") VALUES (";

            for (int i = 0; i < data.Length; i++)
            {
                if (data[i] is string)
                {
                    sql += $"'{data[i]}'";
                }
                else if (data[i] is bool)
                {
                    sql += (bool)data[i] ? "TRUE" : "FALSE";
                }
                else
                {
                    sql += data[i]; // Assume other types can be directly inserted
                }

                // Add comma if it's not the last value
                if (i < data.Length - 1)
                {
                    sql += ", ";
                }
            }

            sql += ");";

            return sql;
        }

        // Check for Table Existance in the database
        private bool TableExists(VerticaConnection connection, string tableName)
        {
            using (VerticaCommand command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(*) FROM tables WHERE table_name = '{tableName}'";
                int count = Convert.ToInt32(command.ExecuteScalar());

                return count > 0;
            }
        }

        // Check for file name and size validations
        private bool IsFileValid(string fileName, string filePath)
        {
            Dictionary<string, Tuple<long, long>> formatSizeRange = new Dictionary<string, Tuple<long, long>> {
                {"SOEM1_TN_RADIO_LINK_POWER", new Tuple<long, long>(0, 1024 * 1024 * 15) },
                {"SOEM1_TN_RFInputPower", new Tuple<long, long>(0, 1024 * 1024 * 30)  }
            };
            KeyValuePair<string, Tuple<long, long>> matchingFormat = new KeyValuePair<string, Tuple<long, long>>();  // Initialize to an empty KeyValuePair


            long fileSize = new FileInfo(filePath).Length;


            foreach (var format in formatSizeRange)
            {
                string formatConstant = format.Key;

                if (fileName.StartsWith(formatConstant))
                {
                    matchingFormat = format;
                    break;
                }
            }

            if (matchingFormat.Equals(default(KeyValuePair<string, Tuple<long, long>>)))
            {
                Console.WriteLine($">> File {fileName} does not match any of the supported formats. Ignoring the file.");
                File.Delete(filePath);
                return false;
            }

            Tuple<long, long> sizeRange = matchingFormat.Value;

            if (fileName.StartsWith(matchingFormat.Key) && fileSize >= sizeRange.Item1 && fileSize <= sizeRange.Item2)
            {
                return true;

            }

            if (!fileName.StartsWith(matchingFormat.Key))
            {
                Console.WriteLine($">> File {fileName} does not match the correct format. Ignoring the file.");
                _logger.LogError($">> File {fileName} does not match the correct format. Ignoring the file.");
                File.Delete(filePath);
            }
            else if (!(fileSize >= sizeRange.Item1 && fileSize <= sizeRange.Item2))
            {
                Console.WriteLine($">> File {fileName} does not match the correct size. Ignoring the file.");
                _logger.LogError($">> File {fileName} does not match the correct size. Ignoring the file.");
                File.Delete(filePath);
            }

            return false;
        }

        // Check if the file finished copying if not wait for it 
        private bool IsFileStillBeingCopied(string filePath)
        {
            try
            {
                using (FileStream stream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
                {
                    // The file is not exclusively locked, indicating it's not being copied
                    return false;
                }
            }
            catch (IOException)
            {
                // The file is exclusively locked, indicating it's still being copied
                return true;
            }
        }


        // Wait for the file to finish copying
        private void WaitForFileCopyCompletion(string filePath)
        {
            string previousChecksum = null;
            string currentChecksum = CalculateFileChecksum(filePath);

            while (previousChecksum != currentChecksum)
            {
                previousChecksum = currentChecksum;
                Thread.Sleep(1000); // Synchronously wait for a second before checking again
                currentChecksum = CalculateFileChecksum(filePath);
            }

            // File is fully copied and its checksum remains the same
            Console.WriteLine($">> File copy completed: {filePath}");
            fileChecksums[filePath] = currentChecksum;
        }

        private string CalculateFileChecksum(string filePath)
        {
            using (var sha256 = SHA256.Create())
            using (var fileStream = File.OpenRead(filePath))
            {
                byte[] hash = sha256.ComputeHash(fileStream);
                return BitConverter.ToString(hash).Replace("-", string.Empty);
            }
        }

        // Watch for rename events
        private void OnFileRenamed(object sender, RenamedEventArgs e)
        {
            string oldFilePath = e.OldFullPath;
            string newFilePath = e.FullPath;

            Console.WriteLine("###############################################################################################");
            Console.WriteLine($">> File renamed: {oldFilePath} to {newFilePath}");
        }

        // Watch for change events
        private void OnFileChanged(object sender, FileSystemEventArgs e)
        {
            string filePath = e.FullPath;

            if (!processedFiles.Contains(filePath))
            {
                Console.WriteLine("###############################################################################################");
                Console.WriteLine($">> File changed: {filePath}");

                // Add the file to the HashSet to mark it as processed
                processedFiles.Add(filePath);
            }
        }

        // Watch for deletion events
        private void OnFileDeleted(object sender, FileSystemEventArgs e)
        {
            string filePath = e.FullPath;

            Console.WriteLine("###############################################################################################");
            Console.WriteLine($">> File deleted: {filePath}");
            Console.WriteLine("###############################################################################################");
            _logger.LogInformation($">> File deleted: {filePath}");
        }

        // Watch for errors during watching
        private void OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine("###############################################################################################");
            Console.WriteLine($">> File watcher error: {e.GetException().Message}");
            _logger.LogError($">> File watcher error: {e.GetException().Message}");
        }

        // Process the valid file and move it to the parser folder
        private string ProcessFile(string fileName, string filePath)
        {
            string destinationFolderPath = Path.Combine(Path.GetDirectoryName(filePath), "parser");

            if (!Directory.Exists(destinationFolderPath))
            {
                Directory.CreateDirectory(destinationFolderPath);
            }

            string destinationFilePath = Path.Combine(destinationFolderPath, fileName);

            while (File.Exists(destinationFilePath))
            {
                string filename = Path.GetFileNameWithoutExtension(fileName);
                filename = $"{filename}_{counter.ToString()}{Path.GetExtension(fileName)}";
                destinationFilePath = Path.Combine(destinationFolderPath, filename);
                counter++;
            }

            File.Move(filePath, destinationFilePath);

            Console.WriteLine($">> File {fileName} has been moved successfully to {destinationFolderPath}.");
            _logger.LogInformation($">> File {fileName} has been moved successfully to {destinationFolderPath}.");

            return destinationFilePath;

        }

        public string GetFilePath()
        {
            return totalFilePath;
        }

    }
}
