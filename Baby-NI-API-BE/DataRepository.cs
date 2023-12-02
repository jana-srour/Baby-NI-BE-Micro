using Baby_NI_API_BE.Model;
using Vertica.Data.VerticaClient;

namespace Baby_NI_API_BE
{
    public class DataRepository
    {
        public Data LoadDataFromDatabase()
        {
            List<NeAliasData> hourlyNeAliasDataList = new List<NeAliasData>();
            List<NeAliasData> dailyNeAliasDataList = new List<NeAliasData>();
            List<NeTypeData> hourlyNeTypeDataList = new List<NeTypeData>();
            List<NeTypeData> dailyNeTypeDataList = new List<NeTypeData>();

            Data dataList = new Data();

            using (VerticaConnection connection = new VerticaConnection("Server=10.10.4.231;Database=test;User=bootcamp8;Password=bootcamp82023;"))
            {
                try
                {
                    connection.Open();
            
                    string HourlyNeAliasDataQuery = @"SELECT * FROM HourlyNeAliasViewData;";
                    string DailyNeAliasDataQuery = @"SELECT * FROM DailyNeAliasViewData;";
                    string HourlyNeTypeDataQuery = @"SELECT * FROM HourlyNeTypeViewData;";
                    string DailyNeTypeDataQuery = @"SELECT * FROM DailyNeTypeViewData;";
            
                    hourlyNeAliasDataList = GetNeAliasData(connection, HourlyNeAliasDataQuery);
                    dailyNeAliasDataList = GetNeAliasData(connection, DailyNeAliasDataQuery);
                    hourlyNeTypeDataList = GetNeTypeData(connection, HourlyNeTypeDataQuery);
                    dailyNeTypeDataList = GetNeTypeData(connection, DailyNeTypeDataQuery);
            
            
                    dataList.hourlyNeAliasdata = hourlyNeAliasDataList;
                    dataList.dailyNeAliasdata = dailyNeAliasDataList;
                    dataList.hourlyNeTypedata = hourlyNeTypeDataList;
                    dataList.dailyNeTypedata = dailyNeTypeDataList;
            
                }
                catch (Exception ex)
                {
                    Console.WriteLine($">>>>> An error occurred with the connection with the database. Due to: {ex}");
                }
            }

            //hourlyNeAliasDataList = GetStaticNeAliasData();
            //dailyNeAliasDataList = GetStaticNeAliasData();
            //hourlyNeTypeDataList = GetStaticNeTypeData();
            //dailyNeTypeDataList = GetStaticNeTypeData();
            //
            //dataList.hourlyNeAliasdata = hourlyNeAliasDataList;
            //dataList.dailyNeAliasdata = dailyNeAliasDataList;
            //dataList.hourlyNeTypedata = hourlyNeTypeDataList;
            //dataList.dailyNeTypedata = dailyNeTypeDataList;


            return dataList;
        }

        public List<NeAliasData> GetNeAliasData(VerticaConnection connection, string dataQuery)
        {
            List<NeAliasData> listData = new List<NeAliasData>();

            using (VerticaCommand command = connection.CreateCommand())
            {
                command.CommandText = dataQuery;
                using (VerticaDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        NeAliasData NeAliasData = new NeAliasData
                        {
                            // Populate HourlyData properties from reader fields
                            DateTime_Key = reader.IsDBNull(reader.GetOrdinal("DateTime_Key")) ? DateTime.MinValue : reader.GetDateTime(reader.GetOrdinal("DateTime_Key")),
                            NeAlias = reader.IsDBNull(reader.GetOrdinal("NeAlias")) ? "" : reader.GetString(reader.GetOrdinal("NeAlias")),
                            RSL_Input_Power = reader.IsDBNull(reader.GetOrdinal("RSL_INPUT_POWER")) ? 0.0f : reader.GetFloat(reader.GetOrdinal("RSL_INPUT_POWER")),
                            Max_RX_Level = reader.IsDBNull(reader.GetOrdinal("MAX_RX_LEVEL")) ? 0.0f : reader.GetFloat(reader.GetOrdinal("MAX_RX_LEVEL")),
                            RSL_Deviation = reader.IsDBNull(reader.GetOrdinal("RSL_DEVIATION")) ? 0.0f : reader.GetFloat(reader.GetOrdinal("RSL_DEVIATION"))

                        };

                        listData.Add(NeAliasData);
                    }
                }
            }

            return listData;
        }

        private List<NeAliasData> GetStaticNeAliasData()
        {
            List<NeAliasData> listData = new List<NeAliasData>
            {
                new NeAliasData
                {
                    DateTime_Key = DateTime.Now,
                    NeAlias = "StaticNeAlias1",
                    RSL_Input_Power = 1.0f,
                    Max_RX_Level = 2.0f,
                    RSL_Deviation = 0.5f
                },
                new NeAliasData
                {
                    DateTime_Key = DateTime.Now.AddHours(1),
                    NeAlias = "StaticNeAlias2",
                    RSL_Input_Power = 1.5f,
                    Max_RX_Level = 2.5f,
                    RSL_Deviation = 0.7f
                }
            };

            return listData;
        }

        public List<NeTypeData> GetNeTypeData(VerticaConnection connection, string dataQuery)
        {
            List<NeTypeData> listData = new List<NeTypeData>();

            using (VerticaCommand command = connection.CreateCommand())
            {
                command.CommandText = dataQuery;
                using (VerticaDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        NeTypeData NeTypeData = new NeTypeData
                        {
                            // Populate HourlyData properties from reader fields
                            DateTime_Key = reader.IsDBNull(reader.GetOrdinal("DateTime_Key")) ? DateTime.MinValue : reader.GetDateTime(reader.GetOrdinal("DateTime_Key")),
                            NeType = reader.IsDBNull(reader.GetOrdinal("NeType")) ? "" : reader.GetString(reader.GetOrdinal("NeType")),
                            RSL_Input_Power = reader.IsDBNull(reader.GetOrdinal("RSL_INPUT_POWER")) ? 0.0f : reader.GetFloat(reader.GetOrdinal("RSL_INPUT_POWER")),
                            Max_RX_Level = reader.IsDBNull(reader.GetOrdinal("MAX_RX_LEVEL")) ? 0.0f : reader.GetFloat(reader.GetOrdinal("MAX_RX_LEVEL")),
                            RSL_Deviation = reader.IsDBNull(reader.GetOrdinal("RSL_DEVIATION")) ? 0.0f : reader.GetFloat(reader.GetOrdinal("RSL_DEVIATION"))

                        };

                        listData.Add(NeTypeData);
                    }
                }
            }

            return listData;
        }

        private List<NeTypeData> GetStaticNeTypeData()
        {
            List<NeTypeData> listData = new List<NeTypeData>
            {
                new NeTypeData
                {
                    DateTime_Key = DateTime.Now,
                    NeType = "StaticNeType1",
                    RSL_Input_Power = 3.0f,
                    Max_RX_Level = 4.0f,
                    RSL_Deviation = 1.0f
                },
                new NeTypeData
                {
                    DateTime_Key = DateTime.Now.AddHours(1),
                    NeType = "StaticNeType2",
                    RSL_Input_Power = 3.5f,
                    Max_RX_Level = 4.5f,
                    RSL_Deviation = 1.2f
                }
            };

            return listData;
        }

    }
}

