## Requirements

Java Runtime


## How to Build

``` bash
git clone https://github.com/anjlab/csv2db.git csv2db

cd csv2db

chmod +x gradlew

./gradlew build
```

## How to Use

``` bash
chmod +x ./build/libs/run.sh

./build/libs/run.sh --help

usage: ./run.sh
 -c,--config <arg>            Configuration file
 -d,--driverClass             JDBC driver class name
 -h,--help                    Prints this help
 -i,--input <arg>             Input CSV file
 -l,--connectionUrl           JDBC connection URL
 -m,--mode                    Operation mode (INSERT, MERGE, INSERTONLY)
 -p,--password                Connection password
 -t,--numberOfThreads <arg>   Number of threads
 -u,--username                Connection username
```

Command line options take precedence over values from config file.

### Pick right database driver

This distribution shipped with latest PostgreSQL 9.x JDBC driver.

If you need to connect to different RDBMS try either:

  * (re)build `csv2db` with `-PjdbcDriver=<groupId:artifactId:version>` key.
 
    For example, to get latest 5.x MySQL Connector/J driver run build with:

  ```
./gradlew -PjdbcDriver=mysql:mysql-connector-java:5+ build
```

  * or manually put JDBC driver to `./build/libs/lib`.

You can find names of JDBC drivers in [Maven Central](http://search.maven.org).

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
    "extend": "parent-config.json",
    "operationMode": "MERGE",
    "driverClass": "org.apache.derby.jdbc.EmbeddedDriver",
    "connectionUrl": "jdbc:derby:memory:myDB;create=true",
    "connectionProperties": {
        "username": { "function": "connectionUsername" },
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
    "map": {
        "function": "filterRows"
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

Since version 2.2.0 csv2db will use [Nashorn JavaScript engine](http://openjdk.java.net/projects/nashorn/) (built-in to Java 8+) and fallback to [Rhino](https://www.mozilla.org/rhino/) if running on Java 7 and below. There's no switch to control this behavior.

Note: Nashorn is not a drop-in replacement for Rhino, so some things may stop working.

Follow [this guide](https://wiki.openjdk.java.net/display/Nashorn/Rhino+Migration+Guide) to upgrade your scripts from Rhino to Nashorn.

```javascript
"use strict";

importPackage(org.apache.commons.lang3);

//  Map function accepts two arguments:
//  - `row` from CSV file containing only values according to `columnMappings`
//  - `emit` is a callback function that accepts new value of `row` that will
//    be used instead of original value
function filterRows(row, emit) {
    if (row["companies_house_id"]) {
        emit(row)
    }
}

//  For functions used in connection properties
//  first argument is the name of evaluating connection property
function connectionUsername(propertyName) {
    return java.lang.System.getProperty("user.name")
}

function transformAddressLine2(columnName, row) {
    //  row["address_care_of"] available here
    return StringUtils.trimToNull(row[columnName]);
}

function uppercase(columnName, row) {
    return row[columnName].toUpperCase()
}
```

### Format of config file

As shown in example above, config file should be in JSON format.

`extend` allows extending this config definition from parent config. For example, you may extract some common bits from your configs to one shared common file and extend from it. All paths in parent config will be relative to this configuration.

`operationMode` may be one of `MERGE`, `INSERT` or `INSERTONLY`.

In `MERGE` mode the data will be merged to the target table.
This mode requires that the `primaryKeys` property is set.
The tool uses `primaryKeys` to find the data in target database table for each CSV row data.
Query by `primaryKeys` should return exactly 0 or 1 records.
If 0 records will be found, then CVS row will be INSERTed to the database,
otherwise an UPDATE will be performed and data from CSV will overwrite existing database record.

In `INSERTONLY` mode, it operates in a similar way to `MERGE` however the row will be ignored if the Query by `primaryKeys` returns a row.
The effect is that only the rows that don't already exist in the database will be INSERTed in to the table.

`driverClass`, `connectionUrl` and `connectionProperties` are corresponding values from JDBC documentation for your database. Since version 2.1 it is possible to use <a href="#value-definitions">JavaScript function references</a> for connection properties.

`targetTable` is the name of target table in database. The table should exist before import.

`primaryKeys` is the set of primary keys on the table. Only used in `MERGE` and `INSERTONLY` modes.
All `primaryKeys` should be present in `columnMappings` section, which means that CSV should contain `primaryKeys` data.

`columnMappings` defines mapping between zero-based column indexes in CSV and target database table column names.

`transientColumns` defines mapped columns as transient, which means they are only available for JavaScript functions in the `row` argument,
but these columns won't be mapped to target table columns.

`insertValues` and `updateValues` allows providing values for columns that are not in CSV (like in example above with required `id` field, whose value should be taken from PostgreSQL sequence). `insertValues` used in INSERT clauses, `updateValues` used in UPDATE clauses. See <a href="#value-definitions">Value Definitions</a>.

`transform` defines transformation rules for imported data. Right now you can define only one transformation for each column. See <a href="#value-definitions">Value Definitions</a>. Transformations only applied for CSV data and not for the columns defined in `insertValues` and `updateValues`

`map` (optional) defines name of a <a href="#value-definitions">JavaScript function</a>. Every row from input CSV file will be passed through this function. The `map` function must accept two arguments: `row` and `emit`. The value of `row` will be JSON object containing key/values according to `columnMappings`. `emit` is a callback function that accepts new value of `row` that will be used instead of original value in further processing. Client code may invoke `emit` function 0, 1, or many times, acting like a filter or a splitter.

`scripting` defines list of JavaScript file names. The file names are relative to location of the configuration file. You can define your JavaScript functions in these files and reference them from <a href="#value-definitions">Value Definitions</a>.

`forceUpdate` forces executing UPDATE statements for every row even if the data from CSV for this row is the same as in the database table. This may be needed if you want to force applying values from `updateValues` section. Default value is `false` and it is only used in `MERGE` mode.

`ignoreNullPK` ignores any row where any of the PK values in the data are null. This may be needed if you want to top up a reference table from a data table with missing reference values where some of them are null. Default value is `false` and it is only used in `INSERTONLY` mode.

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

Constant value definitions are JSON primitives, and can be strings, numbers or booleans. Use this when you want the same value for all rows of the column:
```json
{
    "insertValues": {
        "data_source": "CompaniesHouse",
        "official_data": true
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
Note that primary key values will be transformed before being checked against the primary key in both `MERGE` and `INSERTONLY` modes, enabling transformation of primary key values.
