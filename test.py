from fast_requests import fast_get
import config


with open('example_dois.txt', 'r') as f:
    doi_list = f.read().splitlines()

urls = [f'https://api.unpaywall.org/v2/{doi}' for doi in doi_list]
params = [{'email': config.my_email} for doi in doi_list]

rate_limit = (10, 1)

(results, message), elapsed = fast_get(urls=urls, params=params, accept_codes=[200], max_retry=3, rate_limit=rate_limit)
