rs.initiate({
    _id: "rs0",
    members: [
        { _id: 0, host: "mongodb-1:27017" },
        { _id: 1, host: "mongodb-2:27017" },
        { _id: 2, host: "mongodb-3:27017" }
    ]
})

db.createUser({
    user: "testUser",
    pwd: "1234",
    roles: [
        { role: "readWrite", db: "data" }
    ]
})