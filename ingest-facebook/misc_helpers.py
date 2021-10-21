#!/usr/bin/env python3

from datetime import datetime, timedelta


def get_dates_range(start_year, start_month, start_day, end_year, end_month, end_day):
    """Get an inclusive range of dates."""
    end_date = datetime(end_year, end_month, end_day)
    start_date = datetime(start_year, start_month, start_day)

    date_range = [start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)]

    return date_range


def flatten_json(y):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            if len(x) != 2:
                for a in x:
                    flatten(x[a], name + a + "_")
            else:
                j = 0
                for a in x:
                    while j < 1:
                        flatten(x["value"], name + x[a] + "_")
                        j += 1
        elif type(x) is list:

            i = 0
            for a in x:
                flatten(a, name)
                i += 1
        else:
            out[str(name[:-1]).replace(".", "_")] = str(x)

    flatten(y)
    return out


def split_list(list_, n):
    """Splits a list by a given number (n) and returns a generator object."""
    list_size = len(list_)
    for chunk in range(0, list_size, n):
        yield list_[chunk : min(chunk + n, list_size)]


def csv_files_to_excel(files, output_filename):
    """Write a set of .csv files as a single .xlsx named filename with one sheet per file."""
    import pandas

    with pandas.ExcelWriter(output_filename, engine="xlsxwriter") as writer:
        for file in files:
            df = pandas.read_csv(file["filename"])
            sheet_name = file["filename"].replace(".csv", "").replace("_", " ").title()

            df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)

            workbook = writer.book
            worksheet = writer.sheets[sheet_name]

            header_format = workbook.add_format({"bold": True, "align": "left", "font_color": "#F27405", "border": 1})

            worksheet.write("A1", file["additional_description"], header_format)

    return output_filename


def csv_files_from_list(filename, headers, array, additional_description):
    """Write a csv file from a list of dicts."""
    import csv

    with open(filename, "w") as file:
        writer = csv.DictWriter(file, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(array)

    return {"filename": filename, "additional_description": additional_description}
