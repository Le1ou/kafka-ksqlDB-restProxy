const http = require('http');

const restHost = process.env.REST_HOST || 'localhost';
const restPort = Number(process.env.REST_PORT || 8082);

function post(topic, records) {
  const body = JSON.stringify({ records });
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: restHost,
      port: restPort,
      path: `/topics/${encodeURIComponent(topic)}`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/vnd.kafka.json.v2+json',
        Accept: 'application/vnd.kafka.v2+json',
        'Content-Length': Buffer.byteLength(body),
      },
    }, (res) => {
      let response = '';
      res.on('data', (chunk) => { response += chunk; });
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`${topic}: HTTP ${res.statusCode}: ${response}`));
          return;
        }
        resolve(JSON.parse(response));
      });
    });
    req.on('error', reject);
    req.end(body);
  });
}

const items = [
  ['ITEM-001', 'Молоко 3.2%', 'DAIRY', 99, true],
  ['ITEM-002', 'Хлеб ржаной', 'BAKERY', 79, true],
  ['ITEM-003', 'Кофе зерновой', 'GROCERY', 749, true],
  ['ITEM-004', 'Яблоки сезонные', 'FRUIT', 159, true],
  ['ITEM-005', 'Сыр выдержанный', 'DAIRY', 429, true],
  ['ITEM-006', 'Вода минеральная', 'DRINKS', 69, true],
  ['ITEM-007', 'Шоколад горький', 'SWEETS', 189, true],
  ['ITEM-008', 'Оливковое масло', 'GROCERY', 899, true],
  ['ITEM-009', 'Йогурт натуральный', 'DAIRY', 129, true],
  ['ITEM-010', 'Чай зелёный', 'DRINKS', 239, false],
  ['ITEM-011', 'Бананы', 'FRUIT', 139, true],
  ['ITEM-012', 'Печенье овсяное', 'SWEETS', 199, true],
];

const itemRecords = items.map(([itemId, itemName, category, price, isActive]) => ({
  key: itemId,
  value: { itemId, itemName, category, price, isActive },
}));

const stores = [101, 102, 103, 104];
const statuses = ['NEW', 'PAID', 'PICKING', 'READY', 'COMPLETED', 'CANCELLED'];
const counterparties = ['C001', 'C002', 'C003', 'C004', 'C005'];
const baseTs = Date.parse('2026-07-15T07:00:00.000Z');
const orderRecords = Array.from({ length: 80 }, (_, index) => {
  const orderId = `ORDER-${String(2001 + index).padStart(4, '0')}`;
  const eventTs = baseTs + index * 45_000;
  const item = items[index % items.length];
  return {
    // REST Proxy JSON v2 serializes this scalar as a JSON string key.
    key: orderId,
    value: {
      orderId,
      storeId: stores[index % stores.length],
      itemId: item[0],
      counterpartyId: counterparties[index % counterparties.length],
      status: statuses[index % statuses.length],
      amount: Number((75 + ((index * 137) % 3900) + (index % 4) * 0.25).toFixed(2)),
      eventTs,
      updatedAt: new Date(eventTs).toISOString(),
    },
  };
});

// A missing item makes the LEFT JOIN NULL case visible in the join exercise.
orderRecords.push({
  key: 'ORDER-UNKNOWN-ITEM',
  value: {
    orderId: 'ORDER-UNKNOWN-ITEM', storeId: 103, itemId: 'ITEM-999',
    counterpartyId: 'C009', status: 'NEW', amount: 555.5,
    eventTs: baseTs + 81 * 45_000,
    updatedAt: new Date(baseTs + 81 * 45_000).toISOString(),
  },
});

const stockKeys = ['103:ITEM-001', '103:ITEM-003', '101:ITEM-002', '104:ITEM-007'];
const stockRecords = Array.from({ length: 32 }, (_, index) => {
  const storeItemId = stockKeys[index % stockKeys.length];
  const [storeId, itemId] = storeItemId.split(':');
  const eventTs = baseTs + (index % 8) * 30_000;
  return {
    key: storeItemId,
    value: {
      storeItemId,
      storeId: Number(storeId),
      itemId,
      quantity: index % 7 === 0 ? -2 : (index % 5) + 1,
      operation: index % 6 === 0 ? 'DELETE' : 'UPSERT',
      eventTs,
      updatedAt: new Date(eventTs).toISOString(),
    },
  };
});

async function main() {
  const results = await Promise.all([
    post('Items', itemRecords),
    post('OrderChanges', orderRecords),
    post('StockChanges', stockRecords),
  ]);
  const errors = results.flatMap((result) => result.offsets || []).filter((entry) => entry.error_code != null);
  if (errors.length) throw new Error(JSON.stringify(errors));
  console.log(`Seeded ${itemRecords.length} items, ${orderRecords.length} orders and ${stockRecords.length} stock events.`);
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
