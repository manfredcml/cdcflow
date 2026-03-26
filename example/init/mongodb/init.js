// Initialize replica set (required for change streams)
rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "localhost:27017" }] });

// Wait for primary election
let attempts = 0;
while (!rs.isMaster().ismaster && attempts < 30) {
  sleep(1000);
  attempts++;
}

if (!rs.isMaster().ismaster) {
  print("ERROR: Failed to elect primary after 30 seconds");
  quit(1);
}

print("Replica set initialized, primary elected");

// Create demo database and collections
const db = db.getSiblingDB("demo");

// Enable pre/post images for change streams (MongoDB 6.0+)
db.createCollection("users", {
  changeStreamPreAndPostImages: { enabled: true },
});
db.createCollection("orders", {
  changeStreamPreAndPostImages: { enabled: true },
});

// Insert sample data
db.users.insertMany([
  { _id: ObjectId(), name: "Alice", email: "alice@example.com", age: 30, active: true },
  { _id: ObjectId(), name: "Bob", email: "bob@example.com", age: 25, active: true },
  { _id: ObjectId(), name: "Charlie", email: "charlie@example.com", age: 35, active: false },
]);

db.orders.insertMany([
  { _id: ObjectId(), user: "Alice", product: "Widget", quantity: 2, price: 19.99 },
  { _id: ObjectId(), user: "Bob", product: "Gadget", quantity: 1, price: 49.99 },
]);

print("Demo data inserted: " + db.users.countDocuments() + " users, " + db.orders.countDocuments() + " orders");
