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

Only few columns listed here, just to show the format of CSV that were used with config below.

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
    "targetTable": "companies",
    "primaryKeys": [
        "companies_house_id"
    ],
    "columnMappings": {
        "0": "name",
        "1": "companies_house_id",
        "2": "address_care_of",
        "4": "address_line_1",
        "5": "address_line_2",
        "6": "post_town",
        "9": "post_code"
    },
    "transientColumns": [
        "address_care_of"
    ],
    "insertValues": {
        "id": { "sql": "nextval('company_id_sequence')" },
        "created_at": { "sql": "current_timestamp" }
    },
    "updateValues": {
        "updated_at": { "sql": "current_timestamp" }
    },
    "transform": {
        "post_code": { "function": "uppercase" },
        "address_line_2" : { "function": "transformAddressLine2" }
    },
    "scripting": [
        "functions.js"
    ],
    "forceUpdate": false,
    "batchSize": 100,
    "csvOptions": {
        "separatorChar": ",",
        "quoteChar": "\"",
        "escapeChar": "\b",
        "skipLines": 1,
        "strictQuotes": false,
        "ignoreLeadingWhiteSpace": true
    }
}
```

#### functions.js
```javascript
"use strict";

importPackage(org.apache.commons.lang3);

function transformAddressLine2(columnName, row) {
    //  row["address_care_of"] available here
    return StringUtils.trimToNull(row[columnName]);
}

function uppercase(columnName, row) {
    return row[columnName].toUpperCase()
}
```

### Usage notes

 * Target table in database should exists.
 * PostgreSQL JDBC driver shipped with this distribution. If you want to connect to another RDBMS you have to put its JDBC driver to ```./build/libs/lib```.

### Format of config file

As shown in example above, config file should be in JSON format.

`operationMode` may be one of `MERGE` or `INSERT`.

In `MERGE` mode the data will be merged to the target table.
This mode requires that `primaryKeys` property was set.
The tool uses `primaryKeys` to find the data in target database table for each CSV row data.
Query by `primaryKeys` should return exactly 0 or 1 records.
If 0 records will be found, then CVS row will be INSERTed to the database,
otherwize UPDATE will be performed and data from CSV will overwrite existing database record.

`driverClass`, `connectionUrl` and `connectionProperties` are corresponding values from JDBC documentation for your database.

`targetTable` is the name of target table in database. The table should exist before import.

`primaryKeys` is the set of primary keys on the table. Only used in `MERGE` mode. All "primaryKeys" should be present in `columnMappings` section, which means that CSV should contain `primaryKeys` data.

`columnMappings` defines mapping between zero-based column indexes in CSV and target database table column names.

`transientColumns` defines mapped columns as transient, which means they are only available for JavaScript functions in the `row` argument, but these columns won't be mapped to target table columns.

`insertValues` and `updateValues` allows providing values for columns that are not in CSV (like in example above with required `id` field, whose value should be taken from PostgreSQL sequence). `insertValues` used in INSERT clauses, `updateValues` used in UPDATE clauses. See <a href="#value-definitions">Value Definitions</a>.

`transform` defines transformation rules for imported data. Right now you can define only one transformation for each column. See <a href="#value-definitions">Value Definitions</a>. Transformations only applied for CSV data and not for the columns defined in `insertValues` and `updateValues`

`scripting` defines list of JavaScript file names. The file names are relative to location of the configuration file. You can define your JavaScript functions in these files and reference them from <a href="#value-definitions">Value Definitions</a>.

`forceUpdate` forces executing UPDATE statements for every row even if the data from CSV for this row is the same as in the database table. This may be needed if you want to force applying values from `updateValues` section. Default value is `false`.

`batchSize` size of INSERT/UPDATE batches. Default value is 100.

`csvOptions` is a set of options supported by http://opencsv.sourceforge.net, here's the defaults:
``` json
{
    "separatorChar": ",",
    "quoteChar": "\"",
    "escapeChar": "\\",
    "skipLines": 0,
    "strictQuotes": false,
    "ignoreLeadingWhiteSpace": true
}
```

#### Value Definitions

Value definitions used to define column values in the `insertValues`, `updateValues` and `transform` blocks.

Value definition may be one of:
  - Constant
  - SQL expression
  - JavaScript function reference

##### Constant

Constant value definitions are JSON primitives. Use this when you want the same value for all rows of the column:
```json
{
    "insertValues": {
        "data_source": "CompaniesHouse"
    }
}
```

##### SQL expression

Result of SQL expression value definition becomes a part of SQL query, in contrast to "Constants" and "JavaScript function references" whose values becomes SQL query parameters.
```json
{
    "insertValues": {
        "id": { "sql": "nextval('company_id_sequence')" }
    }
}
```
SQL expressions are only allowed to be used for `insertValues` and `updateValues`. This is due to performance issues, because INSERT and UPDATE clauses prepared only once and then reused for import of every row.

##### JavaScript function reference

You can reference one of the functions that declared in JavaScript files from `scripting` block:
```json
{
    "transform": {
        "post_code": { "function": "uppercase" }
    }
}
```

Every function will accept two arguments:
  - `columnName` -- name of the column for which this function should produce a value, and
  - `row` -- JSON object representing key-value pairs of currently imported CSV row. Only columns that are in `columnMappings` will be present in this object.

If the function referenced from context of `insertValues` or `updateValues` then `row` argument will be `null`.
