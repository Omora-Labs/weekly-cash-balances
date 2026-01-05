from typing import List

import requests


def get_dates(asset_values: List) -> set:
    return set(av["date"].split("T")[0] for av in asset_values)


def get_exchange_rates_per_date(
    dates: List, currency_pairs: List, currencies: List
) -> List:
    exchange_rates = []
    processed_dates = 0
    for date in dates:
        for pair_id, pair in enumerate(currency_pairs, 1):
            base_curr = currencies[pair["base_currency_id"] - 1]["name"].lower()
            quote_curr = currencies[pair["quote_currency_id"] - 1]["name"].lower()

            response = requests.get(
                f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/{base_curr}.json"
            )
            rates = response.json()[base_curr]

            exchange_rates.append(
                {"currency_pair_id": pair_id, "value": rates[quote_curr], "date": date}
            )
        processed_dates += 1
        print(
            f"Processed exchange rates for date {date}. Processed dates: {processed_dates} Remaining dates: {len(dates) - processed_dates}"
        )
    return exchange_rates
