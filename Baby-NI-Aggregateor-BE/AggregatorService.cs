using Vertica.Data.VerticaClient;

namespace Baby_NI_Aggregateor_BE
{
    public class AggregatorService
    {

        private readonly Microsoft.Extensions.Logging.ILogger _logger;

        private bool isDataReady;
        private string verticaConnection;

        public AggregatorService() { }

        public AggregatorService(Microsoft.Extensions.Logging.ILogger logger, string verticaConnection)
        {
            _logger = logger;

            this.verticaConnection = verticaConnection;

        }

        public void StartAggregation()
        {

            using (VerticaConnection connection = new VerticaConnection(verticaConnection))
            {
                try
                {
                    connection.Open();

                    var table1 = "TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER";
                    var table2 = "TRANS_MW_ERC_PM_WAN_RFINPUTPOWER";
                    if (!TableExists(connection, table1) || !TableExists(connection, table2))
                    {
                        isDataReady = false;
                    }
                    else
                    {
                        isDataReady = true;
                    }


                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error in StartAggregation: {ex.Message}");
                    isDataReady = false;
                }
            }

            if (isDataReady)
            {
                prepareAggregation();
            }

        }

        // Controls the aggregation process
        private void prepareAggregation()
        {
            _logger.LogInformation("###############################################################################################");

            using (VerticaConnection connection = new VerticaConnection(verticaConnection))
            {
                try
                {
                    connection.Open();

                    string[] headers = { }, DataTypes = { };

                    // Hourly Data Aggregation Query
                    string hourlyData = @"
                                        SELECT
                                            RADIO_LINK.Network_SID,
                                            TIME_SLICE(RADIO_LINK.Time, '1', 'HOUR', 'START') AS Time,
                                            RADIO_LINK.DateTime_Key,
                                            RADIO_LINK.NeAlias,
                                            RADIO_LINK.NeType,
                                            MAX(RADIO_LINK.MaxRxLevel) AS MAX_RX_LEVEL,
                                            MAX(RFINPUT.RFInputPower) AS RSL_INPUT_POWER,
                                            CAST(ABS(MAX(RFINPUT.RFInputPower))-(ABS(MAX(RADIO_LINK.MaxRxLevel))) AS DECIMAL(10, 2)) AS RSL_DEVIATION
                                        FROM TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER AS RADIO_LINK
                                        INNER JOIN TRANS_MW_ERC_PM_WAN_RFINPUTPOWER AS RFINPUT
                                        ON RADIO_LINK.Network_SID = RFINPUT.Network_SID
                                        WHERE
                                            RFINPUT.CreationTime < NOW() + INTERVAL '2 minutes' OR RADIO_LINK.CreationTime < NOW()
                                        GROUP BY
                                            RADIO_LINK.Network_SID,
                                            TIME_SLICE(RADIO_LINK.Time, '1', 'HOUR', 'START'),
                                            RADIO_LINK.DateTime_Key,
                                            RADIO_LINK.NeAlias,
                                            RADIO_LINK.NeType;";


                    // Daily Data Aggregation Query
                    string dailyData = @"
                                       SELECT
                                            RADIO_LINK.Network_SID,
                                            TIME_SLICE(RADIO_LINK.Time, '24', 'HOUR', 'START') AS Time,
                                            RADIO_LINK.DateTime_Key,
                                            RADIO_LINK.NeAlias,
                                            RADIO_LINK.NeType,
                                            MAX(RADIO_LINK.MaxRxLevel) AS MAX_RX_LEVEL,
                                            MAX(RFINPUT.RFInputPower) AS RSL_INPUT_POWER,
                                            CAST(ABS(MAX(RFINPUT.RFInputPower))-(ABS(MAX(RADIO_LINK.MaxRxLevel))) AS DECIMAL(10, 2)) AS RSL_DEVIATION
                                        FROM TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER AS RADIO_LINK
                                        INNER JOIN TRANS_MW_ERC_PM_WAN_RFINPUTPOWER AS RFINPUT
                                        ON RADIO_LINK.Network_SID = RFINPUT.Network_SID
                                        WHERE
                                            RFINPUT.CreationTime < NOW() + INTERVAL '2 minutes' OR RADIO_LINK.CreationTime < NOW()
                                        GROUP BY
                                            RADIO_LINK.Network_SID,
                                            TIME_SLICE(RADIO_LINK.Time, '24', 'HOUR', 'START'),
                                            RADIO_LINK.DateTime_Key,
                                            RADIO_LINK.NeAlias,
                                            RADIO_LINK.NeType;";


                    // Hourly NeAlias Data Aggregation Query
                    string hourlyNeAlias = @"
                                        SELECT
                                            DateTime_Key,
                                            NeAlias,
                                            MAX(Max_RX_LEVEL) AS Max_RX_LEVEL,
                                            MAX(RSL_INPUT_POWER) AS RSL_INPUT_POWER,
                                            CAST(ABS(MAX(RSL_INPUT_POWER))-(ABS(MAX(Max_RX_LEVEL))) AS DECIMAL(10, 2)) AS RSL_DEVIATION
                                        FROM
                                            TRANS_MW_AGG_SLOT_HOURLY
                                        GROUP BY
                                            DateTime_Key, NeAlias;";

                    // Daily NeAlias Data Aggregation Query
                    string dailyNeAlias = @"
                                        SELECT
                                            DateTime_Key,
                                            NeAlias,
                                            MAX(Max_RX_LEVEL) AS Max_RX_LEVEL,
                                            MAX(RSL_INPUT_POWER) AS RSL_INPUT_POWER,
                                            CAST(ABS(MAX(RSL_INPUT_POWER))-(ABS(MAX(Max_RX_LEVEL))) AS DECIMAL(10, 2)) AS RSL_DEVIATION
                                        FROM
                                            TRANS_MW_AGG_SLOT_DAILY
                                        GROUP BY
                                            DateTime_Key, NeAlias;";


                    // Hourly NeType Data Aggregation Query
                    string hourlyNeType = @"
                                        SELECT
                                            DateTime_Key,
                                            NeType,
                                            MAX(Max_RX_LEVEL) AS Max_RX_LEVEL,
                                            MAX(RSL_INPUT_POWER) AS RSL_INPUT_POWER,
                                            CAST(ABS(MAX(RSL_INPUT_POWER))-(ABS(MAX(Max_RX_LEVEL))) AS DECIMAL(10, 2)) AS RSL_DEVIATION
                                        FROM
                                            TRANS_MW_AGG_SLOT_HOURLY
                                        GROUP BY
                                            DateTime_Key, NeType;";

                    // Daily NeType Data Aggregation Query
                    string dailyNeType = @"
                                        SELECT
                                            DateTime_Key,
                                            NeType,
                                            MAX(Max_RX_LEVEL) AS Max_RX_LEVEL,
                                            MAX(RSL_INPUT_POWER) AS RSL_INPUT_POWER,
                                            CAST(ABS(MAX(RSL_INPUT_POWER))-(ABS(MAX(Max_RX_LEVEL))) AS DECIMAL(10, 2)) AS RSL_DEVIATION
                                        FROM
                                            TRANS_MW_AGG_SLOT_DAILY
                                        GROUP BY
                                            DateTime_Key, NeType;";


                    // Aggregate the hourly and daily data
                    hourlyDataProcessing("TRANS_MW_AGG_SLOT_HOURLY", hourlyData, connection);
                    dailyDataProcessing("TRANS_MW_AGG_SLOT_DAILY", dailyData, connection);


                    // Aggregate based on NeAlias and time
                    hourlyDataProcessing("TRANS_MW_AGG_SLOT_NEALIAS_HOURLY", hourlyNeAlias, connection);
                    dailyDataProcessing("TRANS_MW_AGG_SLOT_NEALIAS_DAILY", dailyNeAlias, connection);


                    // Aggregate based on NeType and time
                    hourlyDataProcessing("TRANS_MW_AGG_SLOT_NETYPE_HOURLY", hourlyNeType, connection);
                    dailyDataProcessing("TRANS_MW_AGG_SLOT_NETYPE_DAILY", dailyNeType, connection);

                    // Create View to send the data through the API
                    CreateView("HourlyNeAliasViewData", "TRANS_MW_AGG_SLOT_NEALIAS_HOURLY", "NeAlias", connection);
                    CreateView("DailyNeAliasViewData", "TRANS_MW_AGG_SLOT_NEALIAS_DAILY", "NeAlias", connection);
                    CreateView("HourlyNeTypeViewData", "TRANS_MW_AGG_SLOT_NETYPE_HOURLY", "NeType", connection);
                    CreateView("DailyNeTypeViewData", "TRANS_MW_AGG_SLOT_NETYPE_DAILY", "NeType", connection);


                    Console.WriteLine(" >>>>> Aggregation Completed");
                    _logger.LogInformation(">>>>> Aggregation Completed");


                }
                catch (Exception ex)
                {
                    Console.WriteLine($">>>>> An error occured with the connection with the database. Due to: {ex}");
                    _logger.LogError($">>>>> An error occured with the connection with the database. Due to: {ex}");
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
        private string GenerateCreateTableSql(string tableName, string[] headers, string[] data)
        {
            var sql = $"CREATE TABLE {tableName} (";

            // Loop over the headers and the datatypes to read for the creation query
            for (int i = 0; i < headers.Length; i++)
            {
                var columnName = headers[i];
                var datatype = data[i];

                sql += $"\"{columnName}\" {datatype}";

                if (i < headers.Length - 1)
                {
                    sql += ", ";
                }
            }

            sql += ")";

            return sql;
        }

        // Process the data for hourly aggregation
        private void hourlyDataProcessing(string hourlyTableName, string query, VerticaConnection connection)
        {
            string[] headers = { }, DataTypes = { };
            string insertSql = "";


            // Create a command with the SQL query
            using (VerticaCommand command = new VerticaCommand(query, connection))
            {
                using (VerticaDataReader reader = command.ExecuteReader())
                {
                    // Check if the reader has rows
                    if (reader.HasRows)
                    {

                        // Get the number of columns
                        int columnCount = reader.FieldCount;

                        // Construct the INSERT INTO SQL statement with a subquery
                        insertSql = GenerateInsertSqlWithSubquery(query, hourlyTableName);


                        // Create an array to store column names and their datatypes
                        headers = new string[columnCount];
                        DataTypes = new string[columnCount];

                        // Iterate through the result set
                        for (int i = 0; i < columnCount; i++)
                        {
                            // Get the name of the current column
                            headers[i] = reader.GetName(i);
                            DataTypes[i] = reader.GetDataTypeName(i);
                        }

                    }
                }
            }

            // Check if the hourly table exists
            if (TableExists(connection, hourlyTableName))
            {
                Console.WriteLine($">>>>> Table '{hourlyTableName}' exists.");

                // Clear the existing data from the table
                ClearTableData(connection, hourlyTableName);
            }
            else
            {
                Console.WriteLine($">>>>> Table '{hourlyTableName}' does not exist.");


                if (headers != null)
                {
                    // Generate CREATE TABLE statement
                    string createTableSql = GenerateCreateTableSql(hourlyTableName, headers, DataTypes);

                    // Create the table in the database
                    using (VerticaCommand command = connection.CreateCommand())
                    {
                        command.CommandText = createTableSql;
                        command.ExecuteNonQuery();
                        Console.WriteLine(">>>>> Table created successfully.");
                        _logger.LogInformation(">>>>> Table created successfully.");

                    }
                }
                else
                {
                    Console.WriteLine(">>>>> CSV file is empty or does not contain a header row.");
                    _logger.LogError(">>>>> CSV file is empty or does not contain a header row.");
                }
            }

            // Create a command for inserting data
            using (VerticaCommand insertCommand = connection.CreateCommand())
            {
                insertCommand.CommandText = insertSql;
                insertCommand.ExecuteNonQuery();
                Console.WriteLine(">>>>> Data inserted successfully.");
                _logger.LogInformation(">>>>> Data inserted successfully.");
            }

        }

        // Process the data for daily aggregation
        private void dailyDataProcessing(string dailyTableName, string query, VerticaConnection connection)
        {
            string[] headers = { }, DataTypes = { };
            string insertSql = "";


            // Create a command with the SQL query
            using (VerticaCommand command = new VerticaCommand(query, connection))
            {
                using (VerticaDataReader reader = command.ExecuteReader())
                {
                    // Check if the reader has rows
                    if (reader.HasRows)
                    {

                        // Get the number of columns
                        int columnCount = reader.FieldCount;

                        // Construct the INSERT INTO SQL statement with a subquery
                        insertSql = GenerateInsertSqlWithSubquery(query, dailyTableName);

                        // Create an array to store column names and their datatypes
                        headers = new string[columnCount];
                        DataTypes = new string[columnCount];

                        // Iterate through the result set
                        for (int i = 0; i < columnCount; i++)
                        {
                            // Get the name of the current column
                            headers[i] = reader.GetName(i);
                            DataTypes[i] = reader.GetDataTypeName(i);
                        }

                    }
                }
            }

            // Check if the daily table exists
            if (TableExists(connection, dailyTableName))
            {
                Console.WriteLine($">>>>> Table '{dailyTableName}' exists.");

                // Clear the existing data from the table
                ClearTableData(connection, dailyTableName);
            }
            else
            {
                Console.WriteLine($">>>>> Table '{dailyTableName}' does not exist.");


                if (headers != null)
                {
                    // Generate CREATE TABLE statement
                    string createTableSql = GenerateCreateTableSql(dailyTableName, headers, DataTypes);

                    // Create the table in the database
                    using (VerticaCommand command = connection.CreateCommand())
                    {
                        command.CommandText = createTableSql;
                        command.ExecuteNonQuery();
                        Console.WriteLine(">>>>> Table created successfully.");
                        _logger.LogInformation(">>>>> Table created successfully.");
                    }
                }
                else
                {
                    Console.WriteLine(">>>>> CSV file is empty or does not contain a header row.");
                }
            }

            // Create a command for inserting data
            using (VerticaCommand insertCommand = connection.CreateCommand())
            {
                insertCommand.CommandText = insertSql;
                insertCommand.ExecuteNonQuery();
                Console.WriteLine(">>>>> Data inserted successfully.");
                _logger.LogInformation(">>>>> Data inserted successfully.");
            }

        }

        // Clear table data
        private void ClearTableData(VerticaConnection connection, string tableName)
        {
            using (VerticaCommand command = connection.CreateCommand())
            {
                // Use DELETE to clear all data from the table
                string deleteSql = $"DELETE FROM {tableName}";
                command.CommandText = deleteSql;
                command.ExecuteNonQuery();
                Console.WriteLine($">>>>> Data cleared from '{tableName}'");
            }
        }

        // Create Insert date Query 
        private string GenerateInsertSqlWithSubquery(string query, string tableName)
        {
            // Construct the INSERT INTO SQL statement with the subquery
            string insertSql = $@"
                                INSERT INTO {tableName}
                                {query}
                                ";

            return insertSql;
        }

        // Create View table
        private void CreateView(string viewName, string tableName, string Ne, VerticaConnection connection)
        {

            // Check if the view already exists
            if (ViewExists(connection, viewName))
            {
                // Drop the view if it exists
                DropView(connection, viewName);
            }

            // Create the view if it doesn't exist
            string viewSql = $@"
             CREATE VIEW {viewName} AS
             SELECT
                 DateTime_Key,
                 {Ne},
                 RSL_INPUT_POWER,
                 MAX_RX_LEVEL,
                 (RSL_INPUT_POWER - MAX_RX_LEVEL) AS RSL_DEVIATION
             FROM {tableName};
             ";

            using (VerticaCommand viewCommand = connection.CreateCommand())
            {
                viewCommand.CommandText = viewSql;
                viewCommand.ExecuteNonQuery();
                Console.WriteLine($">>>>> View '{viewName}' created successfully.");
                _logger.LogInformation($">>>>> View '{viewName}' created successfully.");
            }


        }

        // Check if View table exists
        private bool ViewExists(VerticaConnection connection, string viewName)
        {
            using (VerticaCommand command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(*) FROM views WHERE table_name = '{viewName}'";
                int count = Convert.ToInt32(command.ExecuteScalar());
                return count > 0;
            }
        }

        // Drop a View table
        private void DropView(VerticaConnection connection, string viewName)
        {
            using (VerticaCommand command = connection.CreateCommand())
            {
                command.CommandText = $"DROP VIEW {viewName}";
                command.ExecuteNonQuery();
            }
        }


    }
}
