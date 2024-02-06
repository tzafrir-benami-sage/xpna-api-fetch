import crypto from 'node:crypto';

const apiUrl = process.env.API_URL;
const defaultScenarioId = process.env.DEFAULT_SCENARIO_ID ?? '411cbb1e-6215-4713-b593-47db7a288193';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const randomSleep = async (min: number, max: number) => {
  await sleep(crypto.randomInt(min, max));
};

const getCollection = async (collection: string, limit: number, offset: number) => {
  const startTime = Date.now();

  const url = `${apiUrl}/objects/${collection}?limit=${limit}&offset=${offset}&fields=["doc","id","updated_at"]`;
  const res = await fetch(url, { method: 'GET' });
  
  const body = await res.json();

  if (res.status !== 200) {
    console.error(`Error fetching collection: ${url}: ${res.status}`, body);
    return [];
  }

  const { items } = body as { items: { [key: string]: unknown }[] };

  const endTime = Date.now();
  console.info(`Download [${collection}] collection (${items.length} items) in ${Math.floor(
    endTime - startTime)} milliseconds`
  );

  return items;
};

const getDocument = async (collection: string, id: string) => {
  const startTime = Date.now();

  const url = `${apiUrl}/objects/${collection}/${id}`;
  const res = await fetch(url, { method: 'GET' });
  
  const body = await res.json();

  if (res.status !== 200) {
    console.error(`Error fetching document: ${url}: ${res.status}`, body);
    return {};
  }

  const { item } = body as { item: { [key: string]: unknown } };

  const endTime = Date.now();
  console.info(`Download [${collection}] document (${item.id}) in ${Math.floor(
    endTime - startTime)} milliseconds`
  );

  return item;
};

const queryCollection = async (collection: string, limit: number, offset: number) => {
  const startTime = Date.now();

  const url = `${apiUrl}/query`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      collection,
      limit,
      offset,
      filter: { id: { $ne: 'this-is-invalid-id' } },
      sort: { updated_at: 'desc' },
      fields: ['doc', 'id', 'created_at', 'updated_at']
    })
  });
  
  const body = await res.json();

  if (res.status !== 200) {
    console.error(`Error query collection: ${url}: ${res.status}`, body);
    return [];
  }

  const { items } = body as { items: { [key: string]: unknown }[] };

  const endTime = Date.now();
  console.info(`Query [${collection}] collection (${items.length} docs) in ${Math.floor(
    endTime - startTime)} milliseconds`
  );

  return items;
};

const pullCollection = async (collection: string, limit: number, minUpdatedAt: number) => {
  const startTime = Date.now();

  const url = `${apiUrl}/pull/${collection}?minUpdatedAt=${minUpdatedAt}&limit=${limit}`;
  const res = await fetch(url, { method: 'GET' });
  
  const body = await res.json();

  if (res.status !== 200) {
    console.error(`Error pull collection: ${url}: ${res.status}`, body);
    return [];
  }

  const items = body as { [key: string]: unknown }[];

  const endTime = Date.now();
  console.info(`Pull [${collection}] collection (${items.length} items) in ${Math.floor(
    endTime - startTime)} milliseconds`
  );

  return items;
};

const pushCollection = async (collection: string, items: { [key:string]: unknown}[]) => {
  const startTime = Date.now();

  const docs = items.map(item => {
    const api = item.api_test as number ?? 0;
    return {
      newDocumentState: { ...item, ...{ api_test: api + 1 } },
      assumedMasterState: item
    }
  });

  // create conflict in first doc
  docs[0].assumedMasterState.conflict = true;

  const url = `${apiUrl}/push/${collection}`;
  const res = await fetch(url,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        docs
      })
  });
  
  const body = await res.json();

  if (res.status !== 200) {
    console.error(`Error push collection: ${url}: ${res.status}`, body);
    return [];
  }

  const conflicts = body as { [key: string]: unknown }[];

  const endTime = Date.now();
  console.info(`Push collection ${collection} (${conflicts}) in ${Math.floor(
    endTime - startTime)} milliseconds`
  );

  return conflicts;
};

const pushDefaultScenario = async (scenario: { [key:string]: unknown}) => {
  const startTime = Date.now();

  const api = scenario.api_test as number ?? 0;
  const url = `${apiUrl}/push/scenario`;
  const res = await fetch(url,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        docs: [
          {
            newDocumentState: { ...scenario, ...{ api_test: api + 1 }},
            assumedMasterState: scenario
          }
        ]
      })
  });
  
  const body = await res.json();

  if (res.status !== 200) {
    console.error(`Error push collection: ${url}: ${res.status}`, body);
    return [];
  }

  const conflicts = body as { [key: string]: unknown }[];

  const endTime = Date.now();
  console.info(`Push default scenario (${conflicts}) in ${Math.floor(
    endTime - startTime)} milliseconds`
  );

  return conflicts;
};

const limit = 2000;

(async () => {
  const collections = [
    'change-meta',
    'plan-line',
    'plan-line-formula',
    'formula',
    'dependency-edge',
    'department',
    'location',
    'reporting-period',
    'scenario',
    'change'
  ];

  for (const collection of collections) {
    const items = await getCollection(collection, limit, 0);
    await randomSleep(702, 1003);
    if (items.length > 0) {
      const id = items[items.length - 1].key || items[items.length - 1].id;
      await getDocument(collection, id as string);
      await randomSleep(702, 1003);
    }
  }

  for (const collection of collections) {
    const items = await queryCollection(collection, limit, 0);
    await randomSleep(702, 1003);
    if (items.length > 0) {
      const id = items[0].key || items[0].id;
      await getDocument(collection, id as string);
      await randomSleep(702, 1003);
    }
  }

  for (const collection of collections) {
    const items = await pullCollection(collection, limit, 0);
    await randomSleep(702, 1003);
    if (collection === 'scenario') {
      const defaultScenario = items.filter(item => item.id === defaultScenarioId);
      if (defaultScenario.length > 0) {
        await pushDefaultScenario(defaultScenario[0]);
      }
    }
    if (collection === 'change' && items.length > 0) {
      await pushCollection(collection, items.slice(0, 50));
    }
  }
})();
