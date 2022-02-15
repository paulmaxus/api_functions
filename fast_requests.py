import time
from multiprocessing import cpu_count
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from requests import ConnectionError


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
        def wrapped(reqs, accept_codes: list, max_retry: int = 3, rate_limit: tuple = (5, 1)):
            """ sends (chunks of) parallel get/post requests, adhering to API limits
            :param reqs: list of dicts {id, url, params, (headers), (data)}}
            :param accept_codes: list of API status codes that are acceptable
            :param max_retry: in case of an error
            :param rate_limit: tuple (number of requests, seconds)
            """
            results = {}
            n = rate_limit[0]  # chunk size
            chunks = [reqs[i:i + n] for i in range(0, len(reqs), n)]
            with FuturesSession(max_workers=max_workers) as session:
                chunk_idx = 0
                trial = 0
                while chunk_idx < len(chunks):
                    results_chunk = []
                    start = time.time()
                    # Future requests are run (in parallel) in the background
                    futures = []
                    for req in chunks[chunk_idx]:
                        # catch session errors
                        future = f(session, req['url'],
                                   headers=req.get('headers', None),
                                   params=req.get('params', None),
                                   data=req.get('data', None))
                        future.i = req['id']
                        futures.append(future)
                    # ensure that responses came back before continuing (to not overload the API)
                    for future in as_completed(futures):
                        results_chunk.append(future)
                        # note: results won't be in list order but in order of completion
                    # check for unexpected status codes
                    # plus, .result() forwards ConnectionError
                    api_error = 0
                    try:
                        results_temp = [(future.i, future.result()) for future in results_chunk]
                        status_codes = [result[1].status_code for result in results_temp]
                        if len(set(status_codes).difference(set(accept_codes))) != 0:
                            api_error = 1
                            print(f'unacceptable status codes: {status_codes}')
                    except ConnectionError:
                        api_error = 1
                        print('connection error')
                    if api_error:
                        if trial < max_retry:
                            # retry chunk, otherwise stop
                            trial += 1
                            wait_secs = 30  # arbitrary wait time
                            print(f'waiting {wait_secs} seconds before retrying')
                            time.sleep(wait_secs)
                            continue
                        else:
                            # return results up to now and error messages
                            return results, 'api error'
                    else:
                        for result in results_temp:
                            results[result[0]] = result[1]
                        chunk_idx += 1  # next chunk
                        trial = 0  # reset trial when success
                        # wait the remaining time
                        time.sleep(max(0, rate_limit[1] - (time.time() - start)))
            return results, 'success'
        return wrapped
    return decorator


@execution_timer
@fast_requests(max_workers=cpu_count())
def fast_get(session, url, **kwargs):
    return session.get(url, **kwargs)


@execution_timer
@fast_requests(max_workers=cpu_count())
def fast_post(session, url, **kwargs):
    return session.post(url, **kwargs)

