#!/usr/bin/env python3

import boto3
import json


def get_secret(secret_name, region_name):
    secret_name = secret_name
    region_name = region_name

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response["SecretString"]
    return secret


def get_mappings(database, table):
    glue = boto3.client("glue")
    response = glue.get_table(DatabaseName=database, Name=table)
    mappings = [
        (
            x["Name"],
            x["Type"],
            x["Name"].replace(" ", "_").replace("%", "percentage").replace("(", "").replace(")", "").replace("-", "_").replace("+", "plus"),
            x["Type"],
        )
        for x in response["Table"]["StorageDescriptor"]["Columns"]
    ]
    print(mappings)


def create_sql_db(database, table):
    dt_mappings = {
        "string": "varchar",
        "bigint": "bigint",
        "double": "double precision",
        "date": "date",
        "boolean": "boolean",
        "int": "integer",
    }
    glue = boto3.client("glue")
    response = glue.get_table(DatabaseName=database, Name=table)
    mappings = [(x["Name"], x["Type"]) for x in response["Table"]["StorageDescriptor"]["Columns"]]
    for field in mappings:
        fieldname, datatype = field
        print("{} {},".format(fieldname, dt_mappings[datatype]))

    # print(",".join([x[0] for x in mappings]))
