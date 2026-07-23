# 💾 Atlas Backup Guide

## Overview
This folder is a utility directory designed to help developers migrate data from a cloud MongoDB Atlas instance down into the local Dockerized MongoDB Replica Set.

This is extremely useful when you want to test the data pipeline using real, production-like data, but don't want to accidentally corrupt the production database or incur expensive cloud reads.

## Folder Contents
- `/foodies`: A directory containing BSON and JSON metadata exports from the cloud database.
- `mongorestore` tool is used via a bash script in the root directory to read these files and inject them into your local `localhost:27017` instance.

## How to use
To dump the backup into your local MongoDB, simply run the migration script located in the parent directory:
```bash
cd ../mongodb
./migrate-from-atlas.sh
```
*Note: Make sure the `foodingo-mongodb` docker container is already running before executing this script!*
