import crypto from 'node:crypto';

const apiUrl = process.env.API_URL;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const randomSleep = async (min: number, max: number) => {
  await sleep(crypto.randomInt(min, max));
};

const getCollection = async (collection: string, limit: number, offset: number) => {
  const startTime = Date.now();

  const url = `${apiUrl}/objects/${collection}?limit=${limit}&offset=${offset}&fields=["value","id","updated_at"]&total=${true}`;
  const res = await fetch(url, { method: 'GET' });
  
  const body = await res.json();

  if (res.status !== 200) {
    console.error(`Error fetching collection: ${url}: ${res.status}`, res.headers, body);
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
    console.error(`Error fetching document: ${url}: ${res.status}`, res.headers, body);
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
      fields: ['value', 'id', 'created_at', 'updated_at']
    })
  });
  
  const body = await res.json();

  if (res.status !== 200) {
    console.error(`Error fetching collection: ${url}: ${res.status}`, res.headers, body);
    return [];
  }

  const docs = body as { [key: string]: unknown }[];

  const endTime = Date.now();
  console.info(`Query [${collection}] collection (${docs.length} docs) in ${Math.floor(
    endTime - startTime)} milliseconds`
  );

  return docs;
};

const limit = 2000;

(async () => {
  const collections = [
    'change',
    'change-meta',
    'plan-line',
    'plan-line-formula',
    'formula',
    'dependency-edge',
    'scenario',
    'department',
    'location',
    'reporting-period',
  ];

  for (const collection of collections) {
    const items = await getCollection(collection, limit, 0);
    await randomSleep(338, 702);
    if (items.length > 0) {
      const id = items[items.length - 1].key || items[items.length - 1].id;
      await getDocument(collection, id as string);
      await randomSleep(338, 702);
    }
  }

  for (const collection of collections) {
    const items = await queryCollection(collection, limit, 0);
    await randomSleep(338, 702);
    if (items.length > 0) {
      const id = items[0].key || items[0].id;
      await getDocument(collection, id as string);
      await randomSleep(338, 702);
    }
  }
})();
