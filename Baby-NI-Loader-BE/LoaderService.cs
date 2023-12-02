using System.Security.Cryptography;
using System.Text.RegularExpressions;
using Vertica.Data.VerticaClient;

namespace Baby_NI_Loader_BE
{
    public class LoaderService
    {

        private readonly Microsoft.Extensions.Logging.ILogger _logger;

        public event Action<bool> DataReady;

        private string verticaConnection;
        private Dictionary<string, string> fileChecksums = new Dictionary<string, string>();
        List<string[]> lines = new List<string[]>();

        private string loaderFolder;
        private string filePath;
        private string fileName;
        private string baseFileName;
        private bool isRadioFile;
        private bool dataReceived = false;

        private FileSystemWatcher loaderWatcher;


        public LoaderService(Microsoft.Extensions.Logging.ILogger logger, string loaderFolder, string verticaConnection)
        {
            _logger = logger;

            this.loaderFolder = loaderFolder;
            this.verticaConnection = verticaConnection;

            if (!Directory.Exists(loaderFolder))
            {
                Console.WriteLine(">>>> Loader folder not found.");
                _logger.LogError(">>>> Loader folder not found.");
                return;
            }

            // Initiates the watcher
            loaderWatcher = new FileSystemWatcher();
            loaderWatcher.Path = loaderFolder;
            loaderWatcher.Filter = "*.csv";
            loaderWatcher.EnableRaisingEvents = true;
            loaderWatcher.NotifyFilter = NotifyFilters.FileName;

            // Handle events for the parser folder
            loaderWatcher.Created += OnParserFileCreated;
            loaderWatcher.Error += OnParserError;
        }

        //Controls the Loader process if the file created and ready for loading
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
                Console.WriteLine($">>>> File is still being copied to loader: {filePath}");
                WaitForFileCopyCompletion(filePath); // Wait for the file to be fully copied
            }

            // Check if the file is radio_link or Rfpower to know to which table to insert it into
            isRadioFile = fileName.ToLower().Contains("radio_link_power");

            Console.WriteLine($">>>> File completed: {fileName}");

            // Get the main file name
            //string originalFileName = System.IO.Path.GetFileNameWithoutExtension(fileName);
            baseFileName = ExtractFileName(fileName);
            baseFileName = System.IO.Path.GetFileNameWithoutExtension(baseFileName);

            // Check if the file already exists needs to be force re-executed or no discard it
            using (VerticaConnection connection = new VerticaConnection(verticaConnection))
            {
                connection.Open();

                // Check if the table exists
                if (!TableExists(connection, "FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION"))
                {
                    Console.WriteLine(">>> Table does not exist.");
                    DataBaseConnection();
                    return;
                }

                using (VerticaCommand command = connection.CreateCommand())
                {
                    command.CommandText = $"SELECT * FROM FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION WHERE file_name='{baseFileName}'";

                    using (VerticaDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            reader.Read();

                            // Retrieving value of isParsed column of the file
                            bool isLoadedValue = reader.GetBoolean(reader.GetOrdinal("isLoaded"));

                            // Check for loader re-execution condition
                            if (isLoadedValue)
                            {
                                Console.WriteLine($">>> This file is already Loaded. If you are sure to re-execute it edit file configuration in the database.");
                                _logger.LogCritical($">>> This file is already Loaded. If you are sure to re-execute it edit file configuration in the database.");
                                File.Delete(filePath);
                            }
                            else
                            {
                                DataBaseConnection();
                            }
                        }
                        else
                        {
                            DataBaseConnection();
                        }

                    }
                }

            }
        }

        private string ExtractFileName(string fileName)
        {
            // Check if the file name ends with an underscore and one or two digits
            if (Regex.IsMatch(fileName, @"_\d{1,2}\.csv$"))
            {
                // If yes, remove the underscore and the digits
                fileName = Regex.Replace(fileName, @"_\d{1,2}\.csv$", ".csv");
            }

            // Define a regular expression pattern
            string pattern = @"^(.*?_\d{14})_[\d_]*$";

            // Create a Regex object
            Regex regex = new Regex(pattern);

            // Match the pattern in the file name
            Match match = regex.Match(fileName);

            // Check if the match is successful
            if (match.Success)
            {
                // Extract the matched value
                return match.Groups[1].Value;
            }

            // If no match, return the original file name
            return fileName;
        }




        //Check if the file finished copying if not wait for it 
        private bool IsFileStillBeingCopied(string filePath)
        {
            try
            {
                using (FileStream stream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
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

        //Wait for the file to finish copying
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
            Console.WriteLine($">>>> File copy completed: {filePath}");
            Thread.Sleep(3000);
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

        private void OnParserError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine($">>>> LoaderService: Error: {e.GetException().Message}");
            _logger.LogError($">>>> LoaderService: Error: {e.GetException().Message}");
        }

        // Controls the load of the file to the database
        public void DataBaseConnection()
        {
            using (VerticaConnection connection = new VerticaConnection(verticaConnection))
            {
                try
                {
                    connection.Open();

                    string tableName;
                    string[] headers = { };

                    // Check to which table the file data should be entered
                    if (isRadioFile)
                    {
                        tableName = "TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER";
                    }
                    else
                    {
                        tableName = "TRANS_MW_ERC_PM_WAN_RFINPUTPOWER";
                    }

                    //Read the csv file and save it in a list of array
                    using (StreamReader sr = new StreamReader(filePath))
                    {
                        string line;
                        lines.Clear();

                        while ((line = sr.ReadLine()!) != null)
                        {
                            string[] values = line.Split(',');
                            lines.Add(values);
                        }
                    }

                    // Check if the table exists
                    if (TableExists(connection, tableName))
                    {
                        Console.WriteLine($">>>> Table '{tableName}' exists.");
                    }
                    else
                    {
                        Console.WriteLine($">>>> Table '{tableName}' does not exist.");

                        headers = lines[0];
                        var sample_data = lines[1];

                        if (headers != null && sample_data != null)
                        {
                            // Generate CREATE TABLE statement
                            string createTableSql = GenerateCreateTableSql(tableName, headers, sample_data);

                            // Create the table in the database
                            using (VerticaCommand command = connection.CreateCommand())
                            {
                                command.CommandText = createTableSql;
                                command.ExecuteNonQuery();
                                Console.WriteLine(">>>> Table created successfully.");
                                _logger.LogInformation(">>>> Table created successfully.");
                            }
                        }
                        else
                        {
                            Console.WriteLine(">>>> CSV file is empty or does not contain a header row.");
                            _logger.LogError(">>>> CSV file is empty or does not contain a header row.");
                        }
                    }

                    // Fix the path according to the accepted syntax the database accepts
                    string convertedPath = ConvertPathToUnixStyle(filePath);

                    //Load data into the database after making sure about the correct table
                    using (VerticaCommand command = connection.CreateCommand())
                    {

                        // Use the COPY command to load data into the table
                        //string copyDataSql = $"COPY {tableName} FROM LOCAL '{convertedPath}' DELIMITER ',' DIRECT REJECTED DATA '/home/dbadmin/RejectedData.txt' EXCEPTIONS '/home/dbadmin/S_DB/ExceptionsData.txt';";
                        string copyDataSql = $"COPY {tableName} FROM LOCAL '{convertedPath}' DELIMITER ',' SKIP 1;";
                        command.CommandText = copyDataSql;
                        command.ExecuteNonQuery();
                        Console.WriteLine(">>>> Data loaded successfully.");
                        _logger.LogInformation(">>>> Data loaded successfully.");

                    }

                    var table1 = "TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER";
                    var table2 = "TRANS_MW_ERC_PM_WAN_RFINPUTPOWER";
                    // Send "true" to the main class to say that all data is loaded and ready for aggregation
                    if (!TableExists(connection, table1) || !TableExists(connection, table2))
                    {
                        dataReceived = false;
                    }
                    else
                    {
                        dataReceived = true;
                    }

                    using (VerticaCommand command = connection.CreateCommand())
                    {
                        command.CommandText = $"SELECT * FROM FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION WHERE file_name='{baseFileName}'";

                        // if the file does exist in the table exit this method otherwise insert the file into the table for duplication prevention
                        using (VerticaDataReader reader = command.ExecuteReader())
                        {
                            object[] data = new object[4];
                            string fileExecuted = Path.GetFileNameWithoutExtension(fileName);
                            bool isParsed = false;
                            bool isLoaded = true;
                            bool isReady = dataReceived;

                            if (reader.HasRows)
                            {

                                string updateSql = GenerateUpdateSql("FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION", "isLoaded", isLoaded, baseFileName);

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

                    // Iterate through the data rows (excluding the header)
                    //for (int i = 1; i < lines.Count; i++)
                    //{
                    //    var data = lines[i];
                    //    headers = lines[0];
                    //    // Generate the INSERT INTO query for the current data row
                    //    string insertSql = GenerateInsertSql(tableName, headers!, data);
                    //
                    //    using (VerticaCommand command = connection.CreateCommand())
                    //    {
                    //        command.CommandText = insertSql;
                    //        command.ExecuteNonQuery();
                    //    }
                    //}
                    //
                    //Console.WriteLine(">>>> Data inserted successfully.");

                }
                catch (Exception ex)
                {
                    Console.WriteLine($">>>> An error occured with the connection with the database. Due to: {ex}");
                    _logger.LogError($">>>> An error occured with the connection with the database. Due to: {ex}");
                }

            }

        }

        // Fixing path syntax
        public string ConvertPathToUnixStyle(string filePath)
        {
            return filePath.Replace("\\", "/");
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
        private string GenerateCreateTableSql(string tableName, string[] headers, string[] data)
        {
            var sql = $"CREATE TABLE {tableName} (";

            for (int i = 0; i < headers.Length; i++)
            {
                var columnName = headers[i];
                var sampleValue = data[i];

                // Determine the data type based on the sample value
                string dataType = GetDataType(sampleValue);

                if (columnName.Contains("NeId"))
                {
                    dataType = "FLOAT";
                }
                else if (columnName.ToLower().Contains("time"))
                {
                    dataType = "TIMESTAMP";
                }
                else if (columnName.ToLower().Contains("port") || columnName.ToLower().Contains("slot"))
                {
                    dataType = "VARCHAR(100)";
                }

                sql += $"\"{columnName}\" {dataType}";

                if (i < headers.Length - 1)
                {
                    sql += ", ";
                }
            }

            sql += ");";

            return sql;
        }

        // Gets the DataType of each column based on a sample data value from the file read
        private string GetDataType(string sampleValue)
        {
            // Try parsing the sample value to different data types
            if (sampleValue.Contains("/"))  // Add conditions as needed
            {
                return "VARCHAR(100)";  // Set the data type to VARCHAR
            }
            else if (int.TryParse(sampleValue, out _))
            {
                return "INTEGER";
            }
            else if (double.TryParse(sampleValue, out _))
            {
                return "FLOAT";
            }
            else if (DateTime.TryParse(sampleValue, out _))
            {
                return "TIMESTAMP";
            }
            else
            {
                return "VARCHAR(100)"; // Default to VARCHAR if data type cannot be determined
            }
        }

        // Generates inserto into query
        private string GenerateInsertSql(string tableName, string[] headers, object[] data)
        {
            var sql = $"INSERT INTO {tableName} (";
            sql += string.Join(", ", headers.Select(header => $"\"{header}\"")); // Enclose column names in double quotes
            sql += ") VALUES (";
            sql += string.Join(", ", data.Select(d => $"'{d}'")); // Assuming data values are strings
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


        public bool GetReadyData()
        {
            return dataReceived;
        }

    }

}

