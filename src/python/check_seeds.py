from __future__ import print_function
import argparse
import json
import sys
import time
import requests
import urllib.robotparser
from urllib.parse import urlparse
from requests.exceptions import Timeout
from pickle import NONE

def main():
    """
    Check Seed URLs coming from the Commons Collection API
    """
    default_endpoint = 'https://policycommons.net/api/collections/'

    # Set up and parse the command line args.
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--endpoint', nargs='?', default=default_endpoint, const=default_endpoint, help='The collection endpoint to use' )
    parser.add_argument('-n', type=int, default=0, required=False, help="Stop after n requests")
    args = parser.parse_args()
    clusters = ["us-east-2", "eu-central-1", "ap-northeast-1"]
    timestr = time.strftime("%Y%m%d-%H%M%S")
    timestr = time.strftime("%Y%m%d-%H%M%S")
    tsv_file = open('seed-errors-' + timestr + '.tsv', 'w')
    print("URL\tUUID\tOrg\tCode\tMessage",file=tsv_file)

    startTime = time.time()
    try:
        page = 0
        count = 0
        successes = 0
        failures = 0
        retries = 0
        # Read each CID
        for cluster in clusters:
            this_time = time.time()
            endpoint = args.endpoint
            while endpoint is not None:
                page = page + 1
                print("Checking seeds for cluster {0}, Page {1}, Successes {2}, Failures {3}".format(cluster, page, successes, failures))
                api_response = get_seeds(endpoint, cluster)
                if not api_response:
                    print("No response for request {0}".format(endpoint))
                    retries = retries + 1
                    if retries > 3:
                        print("Failure after three retries {0}".format(endpoint))
                        endpoint = None
                        retries = 0
                        break
                    else:
                        continue
                if 'next' in api_response:
                    endpoint = api_response['next']
                else:
                    endpoint = None

                if 'results' in api_response:
                    for collection in api_response['results']:
                        count = count + 1
                        url = collection['url']
                        title = collection['title']
                        uuid = collection['uuid']
                        org_slug = collection['org']['slug']

                        [success, code, msg] = check_url(url)
                        if success:
                            print(
                                "{0} Verification succeeded for '{1}' from collection {2} org {3}".format(count, url, uuid, org_slug)
                            )
                            successes = successes + 1
                        else:
                            print(
                                "{0} Verification failed for '{1}' from collection {2} org {3} with code {4} and msg '{5}'".format(count, url, uuid, org_slug, code, msg)
                            )
                            print(
                                "{0}\t{1}\t{2}\t{3}\t{4}".format(url, uuid, org_slug, code, msg),
                                file=tsv_file
                            )
                            failures = failures + 1
                        sys.stdout.flush()
                        if args.n > 0 and count >= args.n:
                            break
                    if args.n > 0 and count >= args.n:
                        break

            if args.n > 0 and count >= args.n:
                print("Exiting loop, max requests reached {0}".format(args.n))
                break
    except Exception as e:
        print(e)

    tsv_file.close()
    print('URLs processed {0}, Successes: {1}, Failures: {2}'.format(count, successes, failures))
    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))
    if count > 0:
        print('Average Execution time per request: ' + str(executionTime / count))


def get_seeds(collection_endpoint, cluster):
    params = {
        'sourcing': 'coherencebot',
        'cluster' : cluster
    }
    if "?" in collection_endpoint :
        # The endpoint already has params
        params = None

    try:
        response = requests.get(
            collection_endpoint,
            headers = {
                'Content-Type': 'application/json; charset=utf-8',
                'accept': 'application/json',
                'x-api-key': 'pv=f=-q930xvsfuf(z@o-*ha^!sm2l7vncau_lgr@6m)k$voxk'
            },
            params = params
        )

        status_code = response.status_code
        if status_code == 200:
            response_obj = response.json()
            return response_obj
        else:
            print("Collection request not accepted {0}, {1}, {2}".format(endpoint, cluster, status_code))
            return None
    except Exception as e:
        print("Collection request failed {0}".format(e))
        return None


def check_url(url):
    response_status = False
    response_code = 0
    response_msg = ""

    try:
        response = requests.head(url, timeout=30, allow_redirects=False)
        response_code = response.status_code
        if response_code == 200:
            # We got a 2xx, now check for robot exclusion
            parsed_uri = urlparse(url)
            robots_txt = '{uri.scheme}://{uri.netloc}/robots.txt'.format(uri=parsed_uri)

            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(robots_txt)
            rp.read()
            rp.crawl_delay("*")
            if rp.can_fetch("CoherenceBot", url):
                response_status = True
                response_msg = "Accepted"
            else:
                response_msg = "Robot exclusion"

        elif response_code == 206:
            response_msg = "No content"

        elif response_code > 200 and response_code < 300:
            response_msg = "Other 2xx"

        elif response_code >= 300 and response_code < 400:
            if 'Location' in response.headers:
                response_msg = response.headers['Location']
            else:
                response_msg = "Redirect without location"
                
        elif response_code == 400:
            response_msg = "Bad Request"

        elif response_code == 401:
            response_msg = "Unauthorized"

        elif response_code == 402:
            response_msg = "Payment required"

        elif response_code == 403:
            response_msg = "Forbidden"

        elif response_code == 405:
            response_msg = "Method not allowed"

        elif response_code == 404:
            response_msg = "Not found"

        elif response_code == 406:
            response_msg = "Not acceptable"

        elif response_code >= 400 and response_code < 500:
            response_msg = "Other 4xx"

        elif response_code < 200:
            response_msg = "Partial response"

        elif response_code >= 500:
            response_msg = "Server error"

    except requests.exceptions.Timeout:
        response_msg = "Timeout after 30s"
    except requests.exceptions.TooManyRedirects:
        response_msg = "Too many redirects"
    except requests.exceptions.SSLError:
        response_msg = "SSL error"
    except Exception as e:
        response_msg = e

    return [response_status, response_code, response_msg]

if __name__ == '__main__':
    main()