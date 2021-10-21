#!/usr/bin/env python3

from pyspark.sql.functions import *


def set_custom_value(target_df, read_column, target_column, values, new_value):
    df = target_df.withColumn(target_column, when(col(read_column).isin(values), new_value).otherwise(col(target_column)))

    return df


def google_set_country(target_df):
    df = target_df.withColumn(
        "country",
        when(col("account_id") == "8809700111", "Mexico")
        .when(col("account_id") == "7131842896", "Mexico")
        .when(col("account_id") == "5142225313", "Argentina")
        .when(col("account_id") == "7470075615", "Chile")
        .when(col("account_id") == "7233950276", "Uruguay")
        .when(col("account_id") == "9864688590", "Costa Rica")
        .when(col("account_id") == "6139730345", "Guatemala")
        .when(col("account_id") == "9047913544", "Honduras")
        .when(col("account_id") == "2894285758", "Nicaragua")
        .when(col("account_id") == "6911640413", "Panama")
        .when(col("account_id") == "3806951052", "El Salvador")
        .when(col("account_id") == "5086309825", "Trinidad and Tobago")
        .when(col("account_id") == "6399392278", "Puerto Rico")
        .when(col("account_id") == "9741763601", "Peru")
        .when(col("account_id") == "4348925391", "Jamaica")
        .when(col("account_id") == "9792832374", "Haiti")
        .when(col("account_id") == "9736916886", "Ecuador")
        .when(col("account_id") == "8936082568", "Dominican Republic")
        .when(col("account_id") == "8518402592", "Curazao")
        .when(col("account_id") == "3903960064", "Colombia")
        .when(col("account_id") == "8722360874", "Bahamas")
        .when(col("account_id") == "5277886996", "Bermuda")
        .when(col("account_id") == "8591580897", "Aruba"),
    )

    return df


def set_lumina_id(target_df, source_column):
    luminaPattern = "(?<!\d)[0-9]{4}(?!\d)"
    df = target_df.withColumn("campaign_name_lumina_regex", regexp_replace(col(source_column), "202[01]", ""))
    df = df.withColumn("lumina_id", regexp_extract(col("campaign_name_lumina_regex"), luminaPattern, 0))

    return df


def set_country(target_df, search_column):
    country_pattern = "(?<![a-zA-Z])(AR|ARG|Argentina|Aruba|BO|Bahamas|Barbados|Bermudas|Bermuda|BS|CAY|CH|CL|CO|CR|Chile|Costa Rica|Curacao|DO|Dominican Republic|do|EC|ECU|Ecuador|El Salvador|GT|GrandCayman|Guatemala|HN|hn|Honduras|JA|JM|Jamaica|MX|Mexico|Mx|NI|Nicaragua|PA|PE|PER|PR|PY|Panama|Peru|Puerto Rico|RD|SV|SaintLucia|SaintVicent|sv|TT|Trinidad y Tobago|Trinidad&Tobago|URU|UY|Uruguay)(?![a-zA-Z])"
    df = target_df.withColumn("country_regex", regexp_extract(col(search_column), country_pattern, 1))
    df = df.withColumn(
        "country",
        when(col("country_regex") == "AR", "Argentina")
        .when(col("country_regex") == "ARG", "Argentina")
        .when(col("country_regex") == "Argentina", "Argentina")
        .when(col("country_regex") == "Aruba", "Aruba")
        .when(col("country_regex") == "BO", "Bolivia")
        .when(col("country_regex") == "Bahamas", "Bahamas")
        .when(col("country_regex") == "Barbados", "Barbados")
        .when(col("country_regex") == "Bermuda", "Bermuda")
        .when(col("country_regex") == "Bermudas", "Bermuda")
        .when(col("country_regex") == "BS", "Bermuda")
        .when(col("country_regex") == "CAY", "Cayman Islands")
        .when(col("country_regex") == "CH", "Chile")
        .when(col("country_regex") == "CL", "Chile")
        .when(col("country_regex") == "CO", "Colombia")
        .when(col("country_regex") == "CR", "Costa Rica")
        .when(col("country_regex") == "Chile", "Chile")
        .when(col("country_regex") == "Costa Rica", "Costa Rica")
        .when(col("country_regex") == "Curacao", "Curacao")
        .when(col("country_regex") == "DO", "Dominican Republic")
        .when(col("country_regex") == "Dominican Republic", "Dominican Republic")
        .when(col("country_regex") == "do", "Dominican Republic")
        .when(col("country_regex") == "EC", "Ecuador")
        .when(col("country_regex") == "ECU", "Ecuador")
        .when(col("country_regex") == "Ecuador", "Ecuador")
        .when(col("country_regex") == "El Salvador", "El Salvador")
        .when(col("country_regex") == "GT", "Guatemala")
        .when(col("country_regex") == "GrandCayman", "Cayman Islands")
        .when(col("country_regex") == "Guatemala", "Guatemala")
        .when(col("country_regex") == "HN", "Honduras")
        .when(col("country_regex") == "hn", "Honduras")
        .when(col("country_regex") == "Honduras", "Honduras")
        .when(col("country_regex") == "JA", "Jamaica")
        .when(col("country_regex") == "JM", "Jamaica")
        .when(col("country_regex") == "Jamaica", "Jamaica")
        .when(col("country_regex") == "MX", "Mexico")
        .when(col("country_regex") == "Mexico", "Mexico")
        .when(col("country_regex") == "Mx", "Mexico")
        .when(col("country_regex") == "NI", "Nicaragua")
        .when(col("country_regex") == "Nicaragua", "Nicaragua")
        .when(col("country_regex") == "PA", "Panama")
        .when(col("country_regex") == "PE", "Peru")
        .when(col("country_regex") == "PER", "Peru")
        .when(col("country_regex") == "PR", "Puerto Rico")
        .when(col("country_regex") == "PY", "Paraguay")
        .when(col("country_regex") == "Panama", "Panama")
        .when(col("country_regex") == "Peru", "Peru")
        .when(col("country_regex") == "Puerto Rico", "Puerto Rico")
        .when(col("country_regex") == "RD", "Dominican Republic")
        .when(col("country_regex") == "SV", "El Salvador")
        .when(col("country_regex") == "SaintLucia", "Saint Lucia")
        .when(col("country_regex") == "SaintVicent", "Saint Vicent")
        .when(col("country_regex") == "sv", "El Salvador")
        .when(col("country_regex") == "TT", "Trinidad and Tobago")
        .when(col("country_regex") == "Trinidad y Tobago", "Trinidad and Tobago")
        .when(col("country_regex") == "Trinidad&Tobago", "Trinidad and Tobago")
        .when(col("country_regex") == "URU", "Uruguay")
        .when(col("country_regex") == "UY", "Uruguay")
        .when(col("country_regex") == "Uruguay", "Uruguay")
        .when(col("account_id") == "478922346103747", "Argentina")
        .when(col("account_id") == "2492999897581750", "Chile")
        .when(col("account_id") == "645470382862698", "Colombia")
        .when(col("account_id") == "aciq5u", "Colombia")
        .when(col("account_id") == "474582683479443", "Costa Rica")
        .when(col("account_id") == "605157376902202", "Ecuador")
        .when(col("account_id") == "559831174748149", "El Salvador")
        .when(col("account_id") == "2678605822231450", "Guatemala")
        .when(col("account_id") == "712765479131561", "Honduras")
        .when(col("account_id") == "459514138335909", "Mexico")
        .when(col("account_id") == "2445083972419716", "Nicaragua")
        .when(col("account_id") == "1266788530172308", "Panama")
        .when(col("account_id") == "606229316588636", "Peru")
        .when(col("account_id") == "541440410049583", "Dominican Republic"),
    )
    return df


def set_lumina_country(target_df, source_column):
    """Set the lumina id country in English."""
    df = target_df.withColumn(
        source_column,
        when(col(source_column) == "Costa Rica (USD)", "Costa Rica")
        .when(col(source_column) == "Guatemala (USD)", "Guatemala")
        .when(col(source_column) == "Honduras (USD)", "Honduras")
        .when(col(source_column) == "Jamaica USD", "Jamaica")
        .when(col(source_column) == "Peru (USD)", "Peru")
        .when(col(source_column) == "Trinidad & Tobago (USD)", "Trinidad and Tobago")
        .otherwise(col(source_column)),
    )

    return df
