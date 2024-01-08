const getCollection = async (collection: string, limit: number, offset: number, total = false) => {
  const startTime = Date.now();

  const url = `${process.env.API_URL}/objects/${collection}?limit=${limit}&offset=${offset}&fields=["value","updated_at"]&total=${total}`;
  const res = await fetch(url, { method: 'GET' });
  
  const body = await res.json();

  if (res.status !== 200) {
    console.error(`Error fetching collection: ${url}: ${res.status}`, res.headers, body);
    return { items: [], metadata: { hasMore: false, count: 0, total: 0 } };
  }

  const { items } = body as { items: { [key: string]: unknown }[] };

  const endTime = Date.now();
  console.info(`Download [${collection}] collection (${items.length} items) in ${Math.floor(
    endTime - startTime)} milliseconds`
  );

  return items;
};

const queryCollection = async (collection: string, limit: number, offset: number, total = false) => {
  const startTime = Date.now();

  const url = `${process.env.API_URL}/query`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      collection,
      limit,
      offset,
      filter: { id: { $ne: 'this-is-invalid-id' } },
      sort: { updated_at: 'desc' },
      fields: ['value', 'created_at', 'updated_at']
    })
  });
  
  const body = await res.json();

  if (res.status !== 200) {
    console.error(`Error fetching collection: ${url}: ${res.status}`, res.headers, body);
    return { items: [], metadata: { hasMore: false, count: 0, total: 0 } };
  }

  const docs = body as { [key: string]: unknown }[];

  const endTime = Date.now();
  console.info(`Query [${collection}] collection (${docs.length} docs) in ${Math.floor(
    endTime - startTime)} milliseconds`
  );

  return docs;
};

const limit = 10;

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
    await getCollection(collection, limit, 0);
  }

  for (const collection of collections) {
    await queryCollection(collection, limit, 0);
  }
})();
