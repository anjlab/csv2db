"use strict";

function lowercase(columnName, row) {
    return row[columnName].toLowerCase()
}

function reverseCompanyNumber(columnName, row) {
    return row["company_number"].split("").reverse().join("")
}

function connectionProperty(name) {
    return name + "-from-js";
}