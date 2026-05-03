#!/usr/bin/env python3
"""
Fetches all HubSpot deals and saves them to data/hs_deals.json.
Run by GitHub Actions daily; the dashboard reads this static file.

Required env var: HUBSPOT_TOKEN (HubSpot Private App token)
Required scopes:  crm.objects.deals.read, crm.objects.owners.read,
                  crm.schemas.deals.read (for stage + pipeline label resolution)
"""
import json, os, sys, datetime
import urllib.request, urllib.error

TOKEN = os.environ.get('HUBSPOT_TOKEN', '')
if not TOKEN:
    print('ERROR: HUBSPOT_TOKEN environment variable is not set.', file=sys.stderr)
    sys.exit(1)

HEADERS = {'Authorization': 'Bearer ' + TOKEN}

def hs_get(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

# -- Fetch owners (id -> full name) -------------------------------------------
print('Fetching owners...')
owners_data = hs_get('https://api.hubapi.com/crm/v3/owners?limit=100')
owners = {}
for o in owners_data.get('results', []):
    first = (o.get('firstName') or '').strip()
    last  = (o.get('lastName')  or '').strip()
    name  = (first + ' ' + last).strip() or o.get('email', '') or str(o['id'])
    owners[str(o['id'])] = name
print(f'  {len(owners)} owners loaded')

# -- Fetch pipelines — resolve stage IDs and pipeline IDs to labels -----------
print('Fetching pipelines...')
pipelines_data = hs_get('https://api.hubapi.com/crm/v3/pipelines/deals')
stage_labels    = {}  # stage_id   -> human-readable label
pipeline_labels = {}  # pipeline_id -> pipeline label
for pipe in pipelines_data.get('results', []):
    pipe_id    = str(pipe['id'])
    pipe_label = pipe.get('label', pipe_id)
    pipeline_labels[pipe_id] = pipe_label
    for stage in pipe.get('stages', []):
        stage_labels[str(stage['id'])] = stage.get('label', str(stage['id']))
print(f'  {len(pipeline_labels)} pipelines, {len(stage_labels)} stages loaded')

# -- Fetch all deals (paginated, all stages) ----------------------------------
PROPS = ','.join([
    'dealname',
    'dealstage',
    'pipeline',
    'createdate',
    'lula_deal_source',
    'deal_locs_for_commit_',
    'hubspot_owner_id',
])

print('Fetching deals...')
deals = []
after = None

while True:
    url = f'https://api.hubapi.com/crm/v3/objects/deals?properties={PROPS}&limit=100'
    if after:
        url += f'&after={after}'

    data = hs_get(url)

    for d in data.get('results', []):
        p = d.get('properties', {})

        raw_stores = p.get('deal_locs_for_commit_')
        try:
            stores = max(1, int(float(raw_stores))) if raw_stores else 1
        except (ValueError, TypeError):
            stores = 1

        raw_date = (p.get('createdate') or '')
        date = raw_date[:10] if raw_date else ''

        raw_stage    = str(p.get('dealstage') or '')
        raw_pipeline = str(p.get('pipeline')  or '')

        deals.append({
            'id':       d['id'],
            'date':     date,
            'brand':    (p.get('lula_deal_source') or '').strip() or 'Unknown',
            'stores':   stores,
            'dealname': (p.get('dealname') or '').strip() or '—',
            'owner':    owners.get(str(p.get('hubspot_owner_id') or ''), 'Unassigned'),
            'stage':    stage_labels.get(raw_stage, raw_stage),
            'pipeline': pipeline_labels.get(raw_pipeline, raw_pipeline),
        })

    after = (data.get('paging') or {}).get('next', {}).get('after')
    if not after:
        break

print(f'  {len(deals)} total deals fetched')

# -- Write output -------------------------------------------------------------
output = {
    'fetchedAt': datetime.datetime.utcnow().isoformat() + 'Z',
    'deals': deals,
}

os.makedirs('data', exist_ok=True)
with open('data/hs_deals.json', 'w') as f:
    json.dump(output, f, indent=2)

print(f'Saved data/hs_deals.json ({len(deals)} deals)')

# Print stage + pipeline breakdown for debugging
stages = {}
for d in deals:
    stages[d['stage']] = stages.get(d['stage'], 0) + 1
print('Stage breakdown:', json.dumps(stages, indent=2))

pipelines = {}
for d in deals:
    pipelines[d['pipeline']] = pipelines.get(d['pipeline'], 0) + 1
print('Pipeline breakdown:', json.dumps(pipelines, indent=2))
