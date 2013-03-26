## How to Build

``` bash
chmod +x gradlew

./gradlew clean build

chmod +x ./build/libs/run.sh
```

## How to Use


``` bash
./build/libs/run.sh --help

usage: ./run.sh
 -c,--config <arg>   Configuration file
 -h,--help           Prints this help
 -i,--input <arg>    Input CSV file
```

### Example of CSV file

``` csv
CompanyName, CompanyNumber,RegAddress.CareOf,RegAddress.POBox,RegAddress.AddressLine1, RegAddress.AddressLine2
"! LTD","08209948","","","METROHOUSE 57 PEPPER ROAD","HUNSLET"
"!BIG IMPACT GRAPHICS LIMITED","07382019","","","335 ROSDEN HOUSE","372 OLD STREET"
"!NFERNO LTD.","04753368","","","FIRST FLOOR THAVIES INN HOUSE 3-4","HOLBORN CIRCUS"
```

### Example of config file

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