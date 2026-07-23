// ─── MongoDB Replica Set Initialisation ─────────────────────────────────────
// This script is mounted into the mongodb container and runs on first start.
// It initialises a single-node replica set named "rs0" which enables
// Change Streams — required for Debezium CDC.

try {
    rs.status();
    print("Replica set already initialised — skipping.");
} catch (e) {
    print("Initialising replica set rs0...");
    rs.initiate({
        _id: "rs0",
        members: [{ _id: 0, host: "mongodb:27017" }]
    });
    print("Replica set rs0 initialised successfully.");
}
