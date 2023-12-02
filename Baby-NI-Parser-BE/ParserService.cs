using System.Security.Cryptography;
using Vertica.Data.VerticaClient;

namespace Baby_NI_Parser_BE
{
    public class ParserService
    {
        private readonly Microsoft.Extensions.Logging.ILogger _logger;

        private string parserFolder;
        private Dictionary<string, string> fileChecksums = new Dictionary<string, string>();

        private string filePath;
        private string fileName;
        private string ExcelName;
        private string connectionString;

        private int counter = 2;

        private bool isRadioFile;
        List<string[]> lines = new List<string[]>();

        private FileSystemWatcher parserWatcher;

        public ParserService(Microsoft.Extensions.Logging.ILogger logger, string parserFolder, string connectionString)
        {

            _logger = logger;

            this.parserFolder = parserFolder;
            this.connectionString = connectionString;

            if (!Directory.Exists(parserFolder))
            {
                Console.WriteLine(">>> Parser folder not found.");
                _logger.LogError(">>> Parser folder not found.");
                return;
            }

            // Initiates the watcher
            parserWatcher = new FileSystemWatcher();
            parserWatcher.Path = parserFolder;
            parserWatcher.Filter = "*.txt";
            parserWatcher.EnableRaisingEvents = true;
            parserWatcher.NotifyFilter = NotifyFilters.FileName;

            // Handle events for the parser folder
            parserWatcher.Created += OnParserFileCreated;
            parserWatcher.Error += OnParserError;

        }

        // Starts the parsing once file created detected
        private void OnParserFileCreated(object sender, FileSystemEventArgs e)
        {
            string filePath = e.FullPath;
            string fileName = e.Name!;


            this.fileName = fileName;
            this.filePath = filePath;

            bool stillCopying = IsFileStillBeingCopied(filePath);

            Console.WriteLine("###############################################################################################");
            _logger.LogInformation("###############################################################################################");

            //Check if file still copying if yes wait for it
            if (stillCopying)
            {
                Console.WriteLine($">>> File is still being copied to parser: {filePath}");
                WaitForFileCopyCompletion(filePath); // Wait for the file to be fully copied
            }

            // Check if the file already exists needs to be force re-executed or no discard it
            using (VerticaConnection connection = new VerticaConnection(connectionString))
            {
                connection.Open();

                // Check if the table exists if no just start the parsing
                if (!TableExists(connection, "FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION"))
                {
                    Console.WriteLine(">>> Table does not exist.");
                    StartParsing(filePath);
                    return;
                }

                bool shouldParseFile = true;

                using (VerticaCommand command = connection.CreateCommand())
                {
                    command.CommandText = $"SELECT * FROM FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION WHERE file_name='{Path.GetFileNameWithoutExtension(fileName)}'";

                    using (VerticaDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            reader.Read();

                            // Retrieving value of isParsed column of the file
                            bool isParsedValue = reader.GetBoolean(reader.GetOrdinal("isParsed"));

                            // Check for parser re-execution condition
                            if (isParsedValue)
                            {
                                Console.WriteLine($">>> This file is already parsed. If you are sure to re-execute it edit file configuration in the database.");
                                _logger.LogCritical($">>> This file is already parsed. If you are sure to re-execute it edit file configuration in the database.");
                                shouldParseFile = false;
                            }
                        }

                    }
                }

                if (shouldParseFile)
                {
                    // File not found in the database or not parsed, start parsing
                    StartParsing(filePath);
                }

            }
        }

        //Check if the file finished copying if not wait for it 
            private bool IsFileStillBeingCopied(string filePath, int timeoutMilliseconds = 10000)
            {
                DateTime startTime = DateTime.Now;

                while ((DateTime.Now - startTime).TotalMilliseconds < timeoutMilliseconds)
                {
                    try
                    {
                        // Attempt to open the file with read access. If successful, the file is not being copied.
                        using (FileStream stream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                        {
                            return false;
                        }
                    }
                    catch (IOException)
                    {
                        // File is still being copied; wait for a short interval before the next check.
                        Thread.Sleep(500);
                    }
                }

                // Timeout reached, and the file is still being copied.
                return true;
            }


        //Wait for the file to finish copying
        private void WaitForFileCopyCompletion(string filePath)
        {
            string previousChecksum = null;
            string currentChecksum = CalculateFileChecksum(filePath);

            while (!IsFileReady(filePath) || previousChecksum != currentChecksum)
            {
                previousChecksum = currentChecksum;
                Thread.Sleep(1000); // Synchronously wait for a second before checking again
                currentChecksum = CalculateFileChecksum(filePath);
            }

            // File is fully copied and its checksum remains the same
            Console.WriteLine($">>> File copy completed: {filePath}");
            fileChecksums[filePath] = currentChecksum;
        }

        private bool IsFileReady(string filePath)
        {
            try
            {
                using (FileStream stream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None))
                {
                    // The file is not exclusively locked, indicating it's not being copied
                    return true;
                }
            }
            catch (IOException)
            {
                // The file is exclusively locked, indicating it's still being copied
                return false;
            }
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

        private void OnParserError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine($">>> ParserService: Error: {e.GetException().Message}");
            _logger.LogError($">>> ParserService: Error: {e.GetException().Message}");
        }

        // Controls the parsing process
        public void StartParsing(string filePath)
        {
            string folderPath = Path.GetDirectoryName(filePath)!;
            string filename = Path.GetFileNameWithoutExtension(fileName);

            string newFilename = filename;

            // Create CSV file name, fix it if its duplicate already existing in the folder
            while (File.Exists(Path.Combine(folderPath, "loader", $"{newFilename}.csv")))
            {
                newFilename = $"{filename}_{counter.ToString()}";
                counter++;
            }

            ExcelName = Path.Combine(folderPath, "loader", $"{newFilename}.csv");

            // Checks the file name if radio_link or rfpower to know which parsing to do
            isRadioFile = fileName.ToLower().Contains("radio_link_power");

            Console.WriteLine("#####################################################################################################");
            Console.WriteLine(">>> Updating the Data according to the ISD requirements...");

            // Get the edited column names
            string[] columnsToRemove = GetColumnsToRemove();
            // Read the text file data
            ReadTextFile(columnsToRemove);
            // Write the text file into CSV
            WriteExcelFile();

            _logger.LogInformation(">>> Parsing is completed successfully!");

            Thread.Sleep(1000);
            Console.WriteLine(">>> ISD Requirements Established.");
            Console.WriteLine("#####################################################################################################");


            using (VerticaConnection connection = new VerticaConnection(connectionString))
            {
                connection.Open();

                string[] headers = { "file_name", "isParsed", "isLoaded", "isReady" };
                string[] dataTypes = { "VARCHAR(255)", "BOOLEAN", "BOOLEAN", "BOOLEAN" };

                // Check if table exists if no create it
                if (TableExists(connection, "FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION"))
                {
                    Console.WriteLine($">>> Table 'FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION' exists.");
                }
                else
                {

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
                    if(filename != null)
                    {
                        command.CommandText = $"SELECT * FROM FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION WHERE file_name='{filename}'";

                        // if the file does exist in the table exit this method otherwise insert the file into the table for duplication prevention
                        using (VerticaDataReader reader = command.ExecuteReader())
                        {
                            object[] data = new object[4];
                            string fileExecuted = Path.GetFileNameWithoutExtension(fileName);
                            bool isParsed = true;
                            bool isLoaded = false;
                            bool isReady = false;

                            if (reader.HasRows)
                            {

                                string updateSql = GenerateUpdateSql("FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION", "isParsed", isParsed, fileExecuted);

                                using (VerticaCommand command2 = connection.CreateCommand())
                                {
                                    command2.CommandText = updateSql;
                                    command2.ExecuteNonQuery();
                                }

                                return;
                            }
                            else
                            {
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

        private string GenerateUpdateSql(string tableName, string attr, bool attrValue, string filename)
        {
            var sql = $"UPDATE {tableName} ";
            sql += $"SET {attr} = {attrValue} ";
            sql += $"WHERE file_name = '{filename}';";

            return sql;
        }

        // Check file name to know what columns needs to be removed
        private string[] GetColumnsToRemove()
        {
            if (isRadioFile)
            {
                return new string[] { "NodeName", "Position", "IdLogNum" };
            }
            else
            {
                return new string[] { "Position", "MeanRxLevel1m", "IdLogNum", "FailureDescription" };
            }
        }

        // Check file name to know what columns needs to be added
        private Dictionary<int, string> GetAddedColumns()
        {
            var addedColumns = new Dictionary<int, string>
            {
                { 0, "Network_SID" },
                { 1, "DateTime_Key" }
            };

            if (isRadioFile)
            {
                addedColumns.Add(20, "Link");
                addedColumns.Add(21, "TID");
                addedColumns.Add(22, "FarendTID");
                addedColumns.Add(23, "Slot");
                addedColumns.Add(24, "Port");
                addedColumns.Add(25, "CreationTime");
            }
            else
            {
                addedColumns.Add(17, "Slot");
                addedColumns.Add(18, "Port");
                addedColumns.Add(19, "CreationTime");
            }

            return addedColumns;
        }

        //Read data from the text file
        private void ReadTextFile(string[] colsToDel)
        {
            try
            {
                using (StreamReader sr = new StreamReader(filePath))
                {
                    string line;

                    lines.Clear();

                    while ((line = sr.ReadLine()!) != null)
                    {
                        string[] values = line.Split(',');
                        lines.Add(values);
                    }

                    ParserCleaner parserCleaner = new ParserCleaner(fileName, GetAddedColumns(), isRadioFile);
                    lines = parserCleaner.StartParsingProcess(lines, colsToDel);
                    Console.WriteLine($">>> The text file created is parsed into {ExcelName}");

                }

            }
            catch (IOException ex)
            {
                Console.WriteLine($">>> An error occured while reading from the text file: {ex.Message}");
            }

        }

        //Write the data to the excel file
        private void WriteExcelFile()
        {
            Console.WriteLine(ExcelName);
            try
            {

                using (StreamWriter sw = new StreamWriter(ExcelName))
                {
                    foreach (string[] values in lines)
                    {
                        string line = string.Join(",", values);
                        sw.WriteLine(line);
                    }
                }

            }
            catch (IOException ex)
            {
                Console.WriteLine($">>> An error occured while creating the excel file: {ex.Message}");
            }

        }

        public string GetFile()
        {
            return ExcelName;
        }

    }
}
