"use strict";

function lowercase(columnName, row) {
    return row.get(columnName).toLowerCase()
}

function reverseCompanyNumber(columnName, row) {
    return row.get("company_number").split("").reverse().join("")
}

function connectionProperty(name) {
    return java.lang.System.getProperty("user.name") + "." + name + "-from-js";
}

function testMap(nameValues, emit) {
    // map function may emit() as many rows as it needs
    emit(nameValues);
    emit(nameValues);
}