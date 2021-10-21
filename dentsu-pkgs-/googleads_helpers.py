#!/usr/bin/env python3

from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException


def googleads_list_accessible_accounts(client):
    """
    Return list of accessible customers for the given OAuth credentials.
    Docs at https://developers.google.com/google-ads/api/docs/account-management/listing-accounts
    """
    customer_service = client.get_service("CustomerService")

    try:
        accessible_customers = customer_service.list_accessible_customers()
        googleads_mccs = [mcc for mcc in accessible_customers.resource_names]

        return googleads_mccs

    except GoogleAdsException as ex:
        print("Request with ID {} failed with status {} and includes the " "following errors:".format(ex.request_id, ex.error.code().name))
        for error in ex.failure.errors:
            print('\tError with message "{}".'.format(error.message))
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print("\t\tOn field: {}".format(field_path_element.field_name))
        sys.exit(1)