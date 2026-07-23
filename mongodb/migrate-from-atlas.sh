#!/bin/bash
# ─── Atlas → Local MongoDB Migration Script ──────────────────────────────────
# Run this ONCE to copy your Atlas data to the local Docker MongoDB.
# Prerequisite: mongodump and mongorestore must be installed (mongosh tools).
#   brew install mongodb-database-tools

set -e

ATLAS_URI="mongodb+srv://tapeshchavle:Foodingo%40123@cluster0.jhbbbgh.mongodb.net/foodies?retryWrites=true&w=majority&appName=Cluster0"
LOCAL_URI="mongodb://localhost:27017"
DB_NAME="foodies"
BACKUP_DIR="./atlas-backup"

echo "================================================="
echo " Foodingo Atlas → Local MongoDB Migration"
echo "================================================="
echo ""

# Step 1: Dump from Atlas
echo "[1/3] Dumping database from MongoDB Atlas..."
mongodump --uri="$ATLAS_URI" --db="$DB_NAME" --out="$BACKUP_DIR"
echo "      Dump saved to: $BACKUP_DIR"
echo ""

# Step 2: Wait for local MongoDB to be ready
echo "[2/3] Checking local MongoDB..."
until mongosh "$LOCAL_URI/test" --quiet --eval "db.runCommand('ping').ok" > /dev/null 2>&1; do
    echo "      Waiting for local MongoDB at localhost:27017..."
    sleep 3
done
echo "      Local MongoDB is ready."
echo ""

# Step 3: Restore to local MongoDB
echo "[3/3] Restoring to local MongoDB..."
mongorestore --uri="$LOCAL_URI" --db="$DB_NAME" "$BACKUP_DIR/$DB_NAME" --drop
echo ""

echo "================================================="
echo " Migration complete!"
echo " Collections restored to: $LOCAL_URI/$DB_NAME"
echo ""
echo " NEXT STEP: Update your .env file:"
echo "   Comment out Atlas MONGODB_URI"
echo "   Uncomment local MONGODB_URI:"
echo "   MONGODB_URI=mongodb://localhost:27017/foodies?replicaSet=rs0&directConnection=true"
echo "================================================="
