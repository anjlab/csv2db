## Requirements

Java Runtime


## How to Build

``` bash
chmod +x gradlew

./gradlew clean build

```

## How to Use


``` bash
chmod +x ./build/libs/run.sh

./build/libs/run.sh --help

usage: ./run.sh
 -c,--config <arg>   Configuration file
 -h,--help           Prints this help
 -i,--input <arg>    Input CSV file
```

### Example

#### CSV file

```
CompanyName, CompanyNumber,RegAddress.CareOf,RegAddress.POBox,RegAddress.AddressLine1, RegAddress.AddressLine2
"! LTD","08209948","","","METROHOUSE 57 PEPPER ROAD","HUNSLET"
"!BIG IMPACT GRAPHICS LIMITED","07382019","","","335 ROSDEN HOUSE","372 OLD STREET"
"!NFERNO LTD.","04753368","","","FIRST FLOOR THAVIES INN HOUSE 3-4","HOLBORN CIRCUS"
```

#### Config file

``` json
{
    "operationMode": "MERGE",
    "driverClass": "org.apache.derby.jdbc.EmbeddedDriver",
    "connectionUrl": "jdbc:derby:memory:myDB;create=true",
    "connectionProperties": {
        "username": "sa",
        "password": ""
    },
    "targetTable": "companies_house_records",
    "primaryKeys": [
        "company_number"
    ],
    "columnMappings": {
        "0": "company_name",
        "1": "company_number",
        "4": "company_address_line_1",
        "5": "company_address_line_2"
    },
    "csvOptions": {
        "separatorChar": ",",
        "quoteChar": "\"",
        "escapeChar": "\\",
        "skipLines": 1,
        "strictQuotes": false,
        "ignoreLeadingWhiteSpace": true
    }
}
```

### Usage notes

 * Target table in database should exists.
 * PostgreSQL JDBC driver shipped with this distribution. If you want to connect to another RDBMS you have to put its JDBC driver to ```./build/libs/lib```.

### Format of config file

As shown in example above, config file should be in JSON format.

"operationMode" may be one of MERGE or INSERT.

In MERGE mode the data will be merged to the target table.
This mode requires that "primaryKeys" property were set.
The tool uses "primaryKeys" to find the data in target database table for each CSV row data.
Query by "primaryKeys" should return exactly 0 or 1 records.
If 0 records will be found, then CVS row will be INSERTed to the database,
otherwize UPDATE will be performed and data from CSV will overwrite existing database record.


"driverClass", "connectionUrl" and "connectionProperties" are corresponding values from JDBC documentation for your database.

"targetTable" is the name of target table in database. The table should exist before import.

"primaryKeys" is the set of primary keys on the table. Only used in MERGE mode. All "primaryKeys" should be present in "columnMappings" section, which means that CSV should contain "primaryKeys" data.

"columnMappings" defines mapping between zero-based column indexes in CSV and target database table column names.

"csvOptions" is a set of options supported by http://opencsv.sourceforge.net, here's the defaults:

	- "separatorChar": ",",
    - "quoteChar": "\"",
    - "escapeChar": "\\",
    - "skipLines": 0,
    - "strictQuotes": false,
    - "ignoreLeadingWhiteSpace": true
