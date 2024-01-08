
const apiUrl = process.env.API_URL;

const getCollection = async (collection: string, limit: number, offset: number, total = false) => {
  const fetchTime = Date.now();

  const url = `${apiUrl}/${collection}?limit=${limit}&offset=${offset}&total=${total}`;
  const res = await fetch(url, { method: 'GET' });
  const body = await res.json();

  if (res.status !== 200) {  
    console.error(`Error fetching collection: ${url}: ${res.status}`, res.headers, body);
    return { items: [], metadata: { hasMore: false, count: 0, total: 0 } };
  }

  return body as { items: { [key: string]: unknown }[] };
};

(async () => {
  const collections = [
    'plan-line',
    'plan-line-formula',
    'formula',
    'dependency-edge',
    'department',
    'location',
    'reporting-period',
  ];
  for (const collection of collections) {
    const startTime = Date.now();
    const { items } = await getCollection(collection, 2000, 0);
    const endTime = Date.now();
    console.info(`Download [${collection}] collection (${items.length} items) in ${Math.floor(
      endTime - startTime)} milliseconds`
    );
  }
})();
