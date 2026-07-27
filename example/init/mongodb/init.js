// Initialize replica set (required for change streams).
// Re-running the script against an already-initialized set is not an error.
try {
  rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "localhost:27017" }] });
} catch (e) {
  print("replSetInitiate: " + e);
}

// Wait for primary election
let attempts = 0;
while (!db.hello().isWritablePrimary && attempts < 30) {
  sleep(1000);
  attempts++;
}

if (!db.hello().isWritablePrimary) {
  print("ERROR: Failed to elect primary after 30 seconds");
  quit(1);
}

print("Replica set initialized, primary elected");

// Create demo database and collections.
// NOTE: `const db = db.getSiblingDB(...)` is a self-referencing declaration and
// throws a ReferenceError — the handle must use a different name.
const demoDb = db.getSiblingDB("demo");

// Enable pre/post images for change streams (MongoDB 6.0+).
// Optional: without them, update/delete events fall back to `documentKey`, so
// the `_id` primary key is still captured but the full "before" row is not.
demoDb.createCollection("users", {
  changeStreamPreAndPostImages: { enabled: true },
});
demoDb.createCollection("orders", {
  changeStreamPreAndPostImages: { enabled: true },
});

// Insert sample data
demoDb.users.insertMany([
  { _id: ObjectId(), name: "Alice", email: "alice@example.com", age: 30, active: true },
  { _id: ObjectId(), name: "Bob", email: "bob@example.com", age: 25, active: true },
  { _id: ObjectId(), name: "Charlie", email: "charlie@example.com", age: 35, active: false },
]);

demoDb.orders.insertMany([
  { _id: ObjectId(), user: "Alice", product: "Widget", quantity: 2, price: 19.99 },
  { _id: ObjectId(), user: "Bob", product: "Gadget", quantity: 1, price: 49.99 },
]);

print(
  "Demo data inserted: " +
    demoDb.users.countDocuments() +
    " users, " +
    demoDb.orders.countDocuments() +
    " orders",
);
