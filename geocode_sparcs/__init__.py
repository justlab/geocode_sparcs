#!/usr/bin/env python3

import sys, re, json, multiprocessing
from collections import namedtuple

from tqdm import tqdm
import requests

us_state = 'NY'

pelias_host = 'localhost:4000'

def main():
    for result in lonlats(tuple(
            (addr['line1'], addr['city'], addr['zip'])
            for addr in map(json.loads, sys.stdin))):
        print(json.dumps(dict(lon = result[0], lat = result[1])))

def lonlats(addresses):
    for result in geocode(addresses):
        if result and result['properties']['accuracy'] == 'point':
            assert result['geometry']['type'] == 'Point'
            yield result['geometry']['coordinates']
        else:
            yield (None, None)

def geocode(addresses):
    results = geocode_distinct([
        (line1, city, zipcode)
        for (line1, city, zipcode) in sorted(set(addresses))
        if line1 and city and zipcode and zipcode != 'XXXXX'])
    for a in addresses:
        yield results.get(a)

def geocode_distinct(addresses):
    n_workers = 8
      # On Belle, I don't see an improvement from more workers.

    # Sort by ZIP, then by city. Perhaps this will get us some
    # kind of geographic cache locality.
    addresses = sorted(addresses, key = lambda x: (x[2], x[1]))
    print(f'Geocoding {len(addresses):,} addresses', file = sys.stderr)
    with multiprocessing.Pool(n_workers) as pool:
        return dict(tqdm(
            zip(addresses, pool.imap(geocode1, addresses,
                chunksize = (50 if len(addresses) > 1000 else 1))),
            total = len(addresses),
            unit_scale = True))

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

def pelias(endpoint, **kwargs):
    r = requests.get(
        f'http://{pelias_host}/v1/{endpoint}',
        params = dict(size = 1, **kwargs))
    r.raise_for_status()
    r = r.json()['features']
    assert len(r) <= 1
    return r[0] if r else None

if __name__ == '__main__':
    main(*sys.argv[1:])
