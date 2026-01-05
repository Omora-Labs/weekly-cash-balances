from typing import List

from kairos_assets.ex_rates.utils import get_dates, get_exchange_rates_per_date


def get_exchange_rates(
    asset_values: List, currency_pairs: List, currencies: List
) -> List:
    dates = list(get_dates(asset_values))
    print("Fetching exchange rates for the defined dates")
    ex_rates_per_date = get_exchange_rates_per_date(dates, currency_pairs, currencies)
    return ex_rates_per_date
