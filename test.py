from fast_requests import fast_get
import config


with open('example_dois.txt', 'r') as f:
    doi_list = f.read().splitlines()

upw_urls = [f'https://api.unpaywall.org/v2/{doi}?email={config.my_email}' for doi in doi_list]

rate_limit = (5, 1)

(results, message), elapsed = fast_get(upw_urls, accept_codes=[200], max_retry=3, rate_limit=rate_limit)
