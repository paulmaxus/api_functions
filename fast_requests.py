import time
import functools
from multiprocessing import cpu_count
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession


def execution_timer(f):
    """ decorator to measure a functions execution time
    """
    def inner(*args, **kwargs):
        t0 = time.time()
        results = f(*args, **kwargs)
        t1 = time.time()
        return results, t1-t0
    return inner


def fast_requests(max_workers):
    """ decorator (factory) to speed up requests
    """
    def decorator(f):
        # this is the actual decorator
        @functools.wraps(f)
        def wrapped(urls, accept_codes, max_retry=3, rate_limit=(10, 1), **kwargs):
            # kwargs for optional request header and params
            results = []
            n = rate_limit[0]  # chunk size
            urls_chunks = [urls[i:i+n] for i in range(0, len(urls), n)]
            with FuturesSession(max_workers=max_workers) as session:
                chunk_idx = 0
                trial = 0
                while chunk_idx < len(urls_chunks):
                    results_chunk = []
                    start = time.time()
                    # Future requests are run (in parallel) in the background
                    futures = [f(session, url, **kwargs) for url in urls_chunks[chunk_idx]]
                    # ensure that responses came back before continuing (to not overload the API)
                    for future in as_completed(futures):
                        results_chunk.append(future.result())
                        # note: results won't be in list order but in order of completion
                    # wait the remaining time
                    time.sleep(max(0, rate_limit[1] - (time.time() - start)))
                    # check for unexpected status codes
                    status_codes = [res.status_code for res in results_chunk]
                    if len(set(status_codes).difference(set(accept_codes))) != 0:
                        if trial < max_retry:
                            # retry chunk, otherwise stop
                            trial += 1
                            print('retrying after error')
                            continue
                        else:
                            # return results up to now and error messages
                            return results, f'unacceptable status codes: {status_codes}'
                    else:
                        results = results + results_chunk
                        chunk_idx += 1  # next chunk
                        trial = 0  # reset trial when success
            return results, 'success'
        return wrapped
    return decorator


@execution_timer
@fast_requests(max_workers=cpu_count())
def fast_get(session, url):
    """ sends (chunks of) parallel get requests, adhering to API limits

    params:
        urls: list
        accept_codes: list of status codes that are acceptable
        max_retry: in case of an error
        rate_limit: tuple (number of requests, seconds)
        session is provided by the decorator
    """
    return session.get(url)


@execution_timer
@fast_requests(max_workers=cpu_count())
def fast_post(session, url, headers, payload, data):
    """ sends (chunks of) parallel post requests, adhering to API limits

    params:
        urls: list
        accept_codes: list of status codes that are acceptable
        max_retry: in case of an error
        rate_limit: tuple (number of requests, seconds)
        headers, payload and data for post request
        session is provided by the decorator
    """
    return session.post(url, headers=headers, params=payload, data=data)

