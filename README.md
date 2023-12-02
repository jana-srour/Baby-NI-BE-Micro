# Baby-NI Project

> This project contains multiple project APIs that works as microservices.  <br /><br />
> Watcher project can be run alone as an API where it detects file changes over the monitored directory and checks for any text file added that suites the requirements and not duplicated, if it is suitable it get the file path ready for usage for parsing and other processes.<br /><br />
> The Parser project API will parse the text file saved by the watcher API and manipulate the data to match the ISD requirements and output it in a CSV file. Then it will save the parsed file to be ready for later usage for loader and other steps.<br /><br />
> The Loader project API will get the parsed ready CSV file and load the data into the database under certain conditions and prepares the data for aggregation usage.<br /><br />
> The Aggregator project API will aggregate the data inside the database in a certain manner to have hourly and daily data over certain groups and prepare the data to be held by the API.<br /><br />
> The API in return will handle the data that needs to be send once called which are the Hourly/Daily NeAlias/NeType.<br /><br />


### Install
```bash
git clone https://github.com/jana-srour/Baby_NI_BE.git
```

### Usage
Make sure to change the directories to be watched in the appSettings.json file.<br />
When all is ready, run the program.<br />
Each API can be run alone, or together to form the whole manipulation process of the data.<br />

### Features
##### Logging:
While the program is running it will log the result data into a file with the current date.

##### Re-Execution:
By default, the program doesn't accept duplicate files. if the file needs to be parsed again you need to do this command in the database over the table:
```sql
UPDATE FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION
SET isParsed = false
WHERE file_name = 'your_file_name';
```

and enter the file to start the parsing process, and if you want only to load it set the loader to false and enter the CSV file to load it directly:
```sql
UPDATE FILE_RE_EXEC_DATA_DUPLICATION_PREVENTION
SET isLoaded = false
WHERE file_name = 'your_file_name';
```

### License
Copyright &copy; 2023 Jana Srour.