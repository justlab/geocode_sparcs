#!/usr/bin/env python3

import sys, re, json, multiprocessing, argparse

from tqdm import tqdm
import requests
import inflect

from geocode_sparcs._version import __version__

def cmdline(args):
    p = argparse.ArgumentParser(
        prog = 'geocode-sparcs',
        description = "Geocode addresses from New York State's SPARCS data",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-V', '--version',
        action = 'version', version = '%(prog)s ' + __version__)
    p.add_argument('--pelias-host', metavar = 'HOST',
        type = str, default = 'localhost:4000',
        help = 'hostname of the Pelias server to query')
    p.add_argument('--us-state', metavar = 'STATE',
        type = str, default = 'NY',
        help = 'two-letter abbreviation of the US state to query in')
    p.add_argument('--workers', metavar = 'N',
        type = int, default = 8,
        help = 'number of parallel workers')
    p.add_argument('--lonlats',
        action = 'store_true',
        help = 'return only lons and lats, and only for sufficiently precise matches')
    main(p.parse_args(args), sys.stdin)

def main(options, file_object):
    results = geocode(options, tuple(
        (addr['line1'], addr['city'], addr['zip'])
        for addr in map(json.loads, file_object)))
    if options.lonlats:
        for lon, lat in lonlats(results):
            print(json.dumps(dict(lon = lon, lat = lat)))
    else:
        for result in results:
            print(json.dumps(result))

def lonlats(geocode_results):
    for result in geocode_results:
        if result and result['properties']['accuracy'] == 'point':
            assert result['geometry']['type'] == 'Point'
            yield result['geometry']['coordinates']
        else:
            yield (None, None)

def geocode(options, addresses):
    addresses = [tuple(x and x.lower() for x in a) for a in addresses]
    results = geocode_distinct(options, [
        (line1, city, zipcode)
        for (line1, city, zipcode) in set(addresses)
        if line1 and city and zipcode and zipcode != 'xxxxx'])
    for a in addresses:
        yield results.get(a)

def geocode_distinct(options, addresses):
    # Sort by ZIP, then by city. Perhaps this will get us some
    # kind of geographic cache locality.
    addresses = sorted(addresses, key = lambda x: (x[2], x[1]))
    print(f'Geocoding {len(addresses):,} addresses', file = sys.stderr)
    with multiprocessing.Pool(options.workers, worker_setup, [options]) as pool:
        return dict(tqdm(
            zip(addresses, pool.imap(geocode1, addresses,
                chunksize = (50 if len(addresses) > 1000 else 1))),
            total = len(addresses),
            unit_scale = True))

def worker_setup(options):
    global pelias_host, us_state
    pelias_host, us_state = options.pelias_host, options.us_state

def geocode1(addr):
    line1, city, zipcode = addr
    ok = lambda result, check_zip = False: (
        result and
        result['properties']['accuracy'] == 'point' and
        (not check_zip or
            result['properties'].get('postalcode') == zipcode))
    search = lambda *args: (
        pelias('search', text = ' ,'.join(args)))

    r = search(line1, city, us_state, zipcode)
    if ok(r): return r

    # Try replacing spelled-out numbers.
    new_line1 = numword_re.sub(string = line1, repl = lambda m:
        str(numwords[re.sub('[ -]+', '', m.group(0))]))
    if new_line1 != line1:
        line1 = new_line1
        r = search(line1, city, us_state, zipcode)
    if ok(r): return r

    # Try to trim an apartment number.
    new_line1 = apt_re.sub('', line1)
    if new_line1 != line1:
        line1 = new_line1
        r = search(line1, city, us_state, zipcode)
    if ok(r): return r

    # Try searching without the city name. But don't allow fuzzy
    # matching on the ZIP in this situation; it's too risky.
    r2 = search(line1, us_state, zipcode)
    if ok(r2, check_zip = True): return r2

    # Try without the state, too.
    r2 = search(line1, zipcode)
    if ok(r2, check_zip = True): return r2

    # Try adding a hyphen to any sufficienty long leading number. This
    # is useful for hyphenated building numbers in Queens.
    if (m := re.match(r'\d{4,}', line1)):
        # Put the hyphen before the last 2 digits.
        line1 = m.group()[:-2] + '-' + m.group()[-2:] + line1[m.end():]
        r2 = search(line1, zipcode)
        if ok(r2, check_zip = True): return r2

    # If we've still failed after all this, return the failed result
    # from before we dropped the city and state.
    return r

apt_re = re.compile(flags = re.VERBOSE, pattern = r'''
    [ ]
    (apt [ ]?)?
    [#]?
    (
        \d+ [ ]* -? [ ]* ([a-z] | fl | ph)? |
        ([a-z] | fl | ph) [ ]* -? [ ]* \d* )
    $''')

inflect = inflect.engine()
numwords = {
    inflect.number_to_words(f(i)): i
    for i in range(1, 100)
    for f in (inflect.ordinal, str)}
numword_re = re.compile(r'\b({})\b'.format('|'.join(
    re.sub('[ -]', '[ -]*', o)
    for o in numwords.keys())))
numwords = {re.sub('[ -]', '', k): v for k, v in numwords.items()}

def pelias(endpoint, **kwargs):
    r = requests.get(
        f'http://{pelias_host}/v1/{endpoint}',
        params = dict(size = 1, **kwargs))
    r.raise_for_status()
    r = r.json()['features']
    assert len(r) <= 1
    return r[0] if r else None
