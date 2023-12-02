using System.Text.RegularExpressions;

namespace Baby_NI_Parser_BE
{
    public class ParserCleaner
    {

        private string fileName;
        private readonly bool isRadioFile;

        List<string[]> lines = new List<string[]>();
        List<int> index = new List<int>();

        Dictionary<int, string> AddedColumns = new Dictionary<int, string>();
        List<Dictionary<int, string>> Index_Column = new List<Dictionary<int, string>>();

        public ParserCleaner(string fileName, Dictionary<int, string> AddedColumns, bool isRadioFile)
        {
            this.fileName = fileName;
            this.AddedColumns = AddedColumns;
            this.isRadioFile = isRadioFile;
        }

        // Controls the Data Manipulation during parsing process
        public List<string[]> StartParsingProcess(List<string[]> lines, string[] colsToDel)
        {
            this.lines = lines;

            bool isHeaderLine = true;
            string[] line;

            List<string[]> updatedLines = new List<string[]>();

            for (int i = 0; i < lines.Count; i++)
            {
                line = lines[i];

                if (isHeaderLine)
                {

                    GetDataIndex(line);

                    foreach (string item in colsToDel)
                    {
                        index.Add(Index_Column[0].FirstOrDefault(x => x.Value.Equals(item)).Key);
                    }

                    line = RemoveColumn(line, index);
                    GetDataIndex(line);

                    line = AddColumnHeader(line);
                    GetDataIndex(line);

                    updatedLines.Add(line);

                    isHeaderLine = false;
                }
                else
                {
                    line = DelRowData(line);

                    if (line.Length == 0)
                    {
                        continue; // Skip this line
                    }

                    List<string[]> temp = new List<string[]>();
                    temp = ParseFile(line);

                    foreach (string[] item in temp)
                    {
                        updatedLines.Add(item);
                    }
                }

            }

            return updatedLines;

        }

        //Start the file parsing for each file condition
        private List<string[]> ParseFile(string[] values)
        {
            List<string[]> DataList = new List<string[]>();

            if (isRadioFile)
            {
                values = DelColumnData(values, index);
                values = Create_Network_SID(values);
                values = Create_DateTime_Key(values);
                values = Create_Link(values);
                values = Create_TID_FARENDTID(values);
                DataList = Create_Slot_Port(values);
                DataList = Create_CreationTime(DataList);
            }
            else
            {
                values = DelColumnData(values, index);
                values = Create_Network_SID(values);
                values = Create_DateTime_Key(values);
                DataList = Create_Slot_Port(values);
                DataList = Create_CreationTime(DataList);
            }

            return DataList;

        }

        //Save the Headers with their indexes in a dictionary at each stage to be used in the calculations for the future
        private void GetDataIndex(string[] values)
        {

            Dictionary<int, string> dict = new Dictionary<int, string>();

            for (int i = 0; i < values.Length; i++)
            {
                dict.Add(i, values[i]);
            }

            Index_Column.Add(dict);

        }

        //Add the Newly generated Headers
        private string[] AddColumnHeader(string[] values)
        {

            List<string> editedValues = new List<string>(values);

            foreach (var keyvalue in AddedColumns)
            {

                //Check if the index is out of bounds
                if (keyvalue.Key < 0 || keyvalue.Key > editedValues.Count)
                {
                    //Increase the list's capacity to accomudate new elements
                    if (keyvalue.Key > editedValues.Count)
                    {
                        editedValues.Add(keyvalue.Value);
                    }
                }
                else
                {
                    editedValues.Insert(keyvalue.Key, keyvalue.Value);
                }

            }

            values = editedValues.ToArray();

            return values;
        }

        //Delete rows according to ISD condition requirements
        private string[] DelRowData(string[] values)
        {
            List<string> DataList = new List<string>(values);

            if (isRadioFile)
            {

                int objectIndex = Index_Column[0].FirstOrDefault(x => x.Value.Equals("Object")).Key;
                int failureDescriptionIndex = Index_Column[0].FirstOrDefault(x => x.Value.Equals("FailureDescription")).Key;


                if (objectIndex >= 0 && DataList.Count > objectIndex)
                {
                    if (DataList[objectIndex].Contains("Unreachable Bulk FC"))
                    {
                        DataList.Clear();
                    }
                }

                if (failureDescriptionIndex >= 0 && DataList.Count > failureDescriptionIndex)
                {
                    if (!DataList[failureDescriptionIndex].Contains("-"))
                    {
                        DataList.Clear();
                    }
                }

            }
            else
            {
                int farEndTIDIndex = Index_Column[0].FirstOrDefault(x => x.Value.Equals("FarEndTID")).Key;

                if (farEndTIDIndex >= 0 && DataList.Count > farEndTIDIndex)
                {
                    if (DataList[farEndTIDIndex].Contains("---"))
                    {
                        DataList.Clear();
                    }
                }

            }

            values = DataList.ToArray();

            return values;
        }

        //Remove the headers that needs to be so and get their indexes
        private string[] RemoveColumn(string[] values, List<int> indicesToRemove)
        {

            if (indicesToRemove.Count > 0)
            {
                //Convert the array to a list for removal
                List<string> DataList = new List<string>(values);

                //Remove the words in reverse order to avoid index shifting
                for (int i = indicesToRemove.Count - 1; i >= 0; i--)
                {
                    DataList.RemoveAt(indicesToRemove[i]);
                }

                //Convert the list back to an array
                values = DataList.ToArray();
            }
            else
            {
                Console.WriteLine(">>> No words to remove were found in the array.");
            }

            return values;
        }

        //Add Network SID Data for that newly generated  column
        private string[] Create_Network_SID(string[] values)
        {
            int index1 = Index_Column[1].FirstOrDefault(x => x.Value.Equals("NeAlias")).Key;
            int index2 = Index_Column[1].FirstOrDefault(x => x.Value.Equals("NeType")).Key;

            string CombineNe = values[index1] + values[index2];
            int hash = Math.Abs(CombineNe.GetHashCode());

            //Convert the array to a list for removal
            List<string> DataList = new List<string>(values);

            DataList.Insert(AddedColumns.FirstOrDefault(x => x.Value.Equals("Network_SID")).Key, hash.ToString());

            //Convert the list back to an array
            values = DataList.ToArray();

            return values;

        }

        //Add DateTime Key Data for that newly generated  column
        private string[] Create_DateTime_Key(string[] values)
        {
            // Convert the array to a list for removal
            List<string> DataList = new List<string>(values);

            fileName = Path.GetFileNameWithoutExtension(fileName);

            // Define a regular expression pattern to match the desired part
            string pattern = @"\d{8}_\d{1,6}";

            // Find the first match using the regular expression
            Match match = Regex.Match(fileName, pattern);

            if (match.Success)
            {
                string dateTimePart = match.Value; // Extract the matched part

                // Extract date and time substrings
                string dateSubstring = dateTimePart.Substring(0, 8);
                string timeSubstring = dateTimePart.Substring(9, 6);

                // Manually format the date and time
                string formattedDateTime = $"{dateSubstring.Substring(0, 4)}-{dateSubstring.Substring(4, 2)}-{dateSubstring.Substring(6, 2)} {timeSubstring.Substring(0, 2)}:{timeSubstring.Substring(2, 2)}:{timeSubstring.Substring(4, 2)}";

                // Insert the formatted DateTime into the list
                DataList.Insert(AddedColumns.FirstOrDefault(x => x.Value.Equals("DateTime_Key")).Key, formattedDateTime);
            }

            // Convert the list back to an array
            values = DataList.ToArray();

            return values;
        }


        //Add Link Data for that newly generated  column
        private string[] Create_Link(string[] values)
        {
            //Convert the array to a list for removal
            List<string> DataList = new List<string>(values);

            string text = DataList[Index_Column[2].FirstOrDefault(x => x.Value.Equals("Object")).Key];
            string pattern_Link = @"^(.*?)_";
            string Link = "", slotTemp = "", portTemp = "";

            Dictionary<string, string> slot_port;

            MatchCollection matches_Link = Regex.Matches(text, pattern_Link);

            foreach (Match match in matches_Link)
            {
                Link = match.Groups[1].Value;
                slot_port = Get_Slot_Port(Link, true);

                if (slot_port.Count >= 2)
                {
                    int count = 0;
                    foreach (var item in slot_port)
                    {
                        slotTemp += $"{item.Key}";
                        portTemp = $"{item.Value}";

                        if (count < slot_port.Count - 1)
                        {
                            slotTemp += "+";
                        }
                        count++;
                    }
                }
                else
                {
                    foreach (var item in slot_port)
                    {
                        slotTemp += $"{item.Key}";
                        portTemp = $"{item.Value}";
                    }
                }

                Link = $"{slotTemp}/{portTemp}";

                DataList.Add(Link);

                slotTemp = "";
                portTemp = "";
            }

            //Convert the list back to an array
            values = DataList.ToArray();

            return values;
        }

        //Add TID and FarEnd TID Data for that newly generated column
        private string[] Create_TID_FARENDTID(string[] values)
        {
            //Convert the array to a list for removal
            List<string> DataList = new List<string>(values);

            string text = DataList[Index_Column[2].FirstOrDefault(x => x.Value.Equals("Object")).Key];

            string pattern_TID = @"__(.*?)__";
            string pattern_FarEndTID = @"__([^_]+)$";

            MatchCollection matches_TID = Regex.Matches(text, pattern_TID);
            MatchCollection matches_FarEndTID = Regex.Matches(text, pattern_FarEndTID);

            foreach (Match match in matches_TID)
            {
                string TID = match.Groups[1].Value;
                DataList.Add(TID);
            }

            foreach (Match match in matches_FarEndTID)
            {
                string FARENDTID = match.Groups[1].Value;
                DataList.Add(FARENDTID);
            }

            //Convert the list back to an array
            values = DataList.ToArray();

            return values;
        }

        //Add Slot and Port Data for that newly generated column
        private List<string[]> Create_Slot_Port(string[] values)
        {
            //Convert the array to a list for removal
            List<string> DataList = new List<string>(values);
            List<string[]> strings = new List<string[]>();

            string text = DataList[Index_Column[2].FirstOrDefault(x => x.Value.Equals("Object")).Key];
            string pattern_Link = @"^(.*?)_";
            string Link = "";
            Dictionary<string, string> slot_port = new Dictionary<string, string>();

            MatchCollection matches_Link = Regex.Matches(text, pattern_Link);

            foreach (Match match in matches_Link)
            {
                Link = match.Groups[1].Value;
            }

            if (isRadioFile)
            {

                slot_port = Get_Slot_Port(Link, true);

                foreach (var item in slot_port)
                {
                    List<string> temp = new List<string>(values);
                    temp.Add(item.Key);
                    temp.Add(item.Value);

                    strings.Add(temp.ToArray());
                }

            }
            else
            {

                slot_port = Get_Slot_Port(text, false);

                foreach (var item in slot_port)
                {
                    List<string> temp = new List<string>(values);
                    temp.Add(item.Key);
                    temp.Add(item.Value);

                    strings.Add(temp.ToArray());
                }

            }

            return strings;
        }


        // Generate slot and port values
        private Dictionary<string, string> Get_Slot_Port(string link, bool fileType)
        {
            Dictionary<string, string> slot_port = new Dictionary<string, string>();

            if (fileType)
            {
                string pattern_slot_port = @"/(.*?)/([^/]+)$";
                string[] splitted_string;

                MatchCollection matches_slot_port = Regex.Matches(link, pattern_slot_port);

                foreach (Match match in matches_slot_port)
                {

                    string middle_string = match.Groups[1].Value;
                    string last_string = match.Groups[2].Value;

                    if (middle_string.Contains("+"))
                    {
                        splitted_string = middle_string.Split("+");
                        slot_port.Add(splitted_string[0], last_string);
                        slot_port.Add(splitted_string[1], last_string);
                    }
                    else if (middle_string.Contains("."))
                    {
                        splitted_string = middle_string.Split(".");
                        slot_port.Add(splitted_string[0], splitted_string[1]);
                    }
                    else
                    {
                        slot_port.Add(middle_string, last_string);
                    }
                }
            }
            else
            {

                string pattern_slot = @"(.*?)/(.*?)/([^/]+)$";
                string[] splitted_string;

                MatchCollection matches_slot_port;
                matches_slot_port = Regex.Matches(link, pattern_slot);

                foreach (Match match in matches_slot_port)
                {
                    string first_string = match.Groups[1].Value;
                    string middle_string = match.Groups[2].Value;
                    string last_string = match.Groups[3].Value;

                    if (middle_string.Contains("+"))
                    {
                        splitted_string = middle_string.Split("+");

                        slot_port.Add(first_string + "/" + splitted_string[0] + "+", last_string);
                        slot_port.Add(first_string + "/" + splitted_string[1] + "+", last_string);

                    }
                    else if (middle_string.Contains("."))
                    {
                        splitted_string = middle_string.Split(".");

                        slot_port.Add(first_string + "/" + splitted_string[0] + "+", splitted_string[1]);
                    }
                    else
                    {
                        slot_port.Add(first_string + "/" + middle_string + "+", last_string);
                    }

                }
            }

            return slot_port;
        }

        // Create a creation time for database purposes
        private List<string[]> Create_CreationTime(List<string[]> DataList)
        {
            List<string[]> strings = new List<string[]>();

            // Get the current timestamp
            string currentTimestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

            // Iterate through each string array in the list
            foreach (string[] dataArray in DataList)
            {
                List<string> data = new List<string>(dataArray);
                // Append the current timestamp to the end of the array
                data.Add(currentTimestamp);
                strings.Add(data.ToArray());
            }


            return strings;
        }

        //Delete Columns that needs to be so
        private string[] DelColumnData(string[] values, List<int> indicesToRemove)
        {
            if (indicesToRemove.Count > 0)
            {
                //Convert the array to a list for removal
                List<string> DataList = new List<string>(values);

                //Remove the words in reverse order to avoid index shifting
                for (int i = indicesToRemove.Count - 1; i >= 0; i--)
                {
                    DataList.RemoveAt(indicesToRemove[i]);
                }

                //Convert the list back to an array
                values = DataList.ToArray();
            }
            else
            {
                Console.WriteLine(">>> No words to remove were found in the array.");
            }

            return values;
        }

    }
}
