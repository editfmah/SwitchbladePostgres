//
//  PostgresProvider.swift
//  
//
//  Created by Adrian Herridge on 15/10/2021.
//

import Foundation

import Dispatch
import CryptoSwift
import PostgresClientKit
import Switchblade

//import PostgresClientKit
//
//do {
//    var configuration = PostgresClientKit.ConnectionConfiguration()
//    configuration.host = "127.0.0.1"
//    configuration.database = "example"
//    configuration.user = "bob"
//    configuration.credential = .scramSHA256(password: "welcome1")
//
//    let connection = try PostgresClientKit.Connection(configuration: configuration)
//    defer { connection.close() }
//
//    let text = "SELECT city, temp_lo, temp_hi, prcp, date FROM weather WHERE city = $1;"
//    let statement = try connection.prepareStatement(text: text)
//    defer { statement.close() }
//
//    let cursor = try statement.execute(parameterValues: [ "San Francisco" ])
//    defer { cursor.close() }
//
//    for row in cursor {
//        let columns = try row.get().columns
//        let city = try columns[0].string()
//        let tempLo = try columns[1].int()
//        let tempHi = try columns[2].int()
//        let prcp = try columns[3].optionalDouble()
//        let date = try columns[4].date()
//
//        print("""
//            \(city) on \(date): low: \(tempLo), high: \(tempHi), \
//            precipitation: \(String(describing: prcp))
//            """)
//    }
//} catch {
//    print(error) // better error handling goes here
//}

var ttl_now: Int {
    get {
        return Int(Date().timeIntervalSince1970)
    }
}

internal let kSaltValue = "dfc0e63c6cfd433087055cea149efb1f"

public class PostgresProvider: DataProvider {
    
    public var config: SwitchbladeConfig!
    public var dataTableName = "Data"
    public weak var blade: Switchblade!
    
    fileprivate var host: String
    fileprivate var port: Int = 5432
    fileprivate var username: String
    fileprivate var password: Credential
    fileprivate var database: String
    fileprivate var connections: Int = 2
    fileprivate var ssl: Bool = false
    
    fileprivate var db: Connection!
    
    let decoder: JSONDecoder = JSONDecoder()
    
    public init(host: String, username: String, password: Credential, database: String, connections: Int, ssl: Bool? = nil) {
        
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.connections = connections
        
        if let ssl = ssl {
            self.ssl = ssl
        }
        
    }
    
    public func open() throws {
        
        var configuration = PostgresClientKit.ConnectionConfiguration()
        configuration.host = self.host
        configuration.database = self.database
        configuration.user = self.username
        configuration.credential = self.password
        configuration.ssl = self.ssl
        
        do {
            self.db = try PostgresClientKit.Connection(configuration: configuration)
        }
        catch {
            print("unable to connect to postgres server")
            print(error)
        }
        

        try execute(sql: """
CREATE TABLE IF NOT EXISTS \(dataTableName) (
    partition text,
    keyspace text,
    id text,
    value bytea,
    ttl int,
    timestamp int,
    model text,
    version int,
    filter text,
    PRIMARY KEY (partition,keyspace,id)
);
""", params: [])
        
        // indexes
        try execute(sql: "CREATE INDEX IF NOT EXISTS idx_ttl ON \(dataTableName) (ttl);", params: [])
        try execute(sql: "CREATE INDEX IF NOT EXISTS idx_model ON \(dataTableName) (model,version);", params: [])
        
        DispatchQueue.global(qos: .default).asyncAfter(deadline: .now() + 60, execute: {
            while self.db != nil {
                try? self.execute(sql: "DELETE FROM Data WHERE ttl IS NOT NULL AND ttl < $1;", params: [ttl_now])
                Thread.sleep(forTimeInterval: 60)
            }
        })
        
    }
    
    public func close() throws {
        db.close()
    }
    
    fileprivate func makeId(_ key: String) -> String {
        return key
    }
    
    fileprivate func makeParams(_ params:[Any?]) -> [PostgresValueConvertible?] {
        var values: [PostgresValueConvertible?] = []
        for p in params {
            if let p = p {
                if let value = p as? Data {
                    values.append(PostgresByteA(data: value))
                } else if let value = p as? String {
                    values.append(value)
                } else if let value = p as? Int {
                    values.append(value)
                } else if let value = p as? Int64 {
                    values.append(Int(value))
                } else if let value = p as? UUID {
                    values.append(value.uuidString.lowercased())
                } else if let value = p as? Date {
                    values.append(PostgresTimestamp(date: value, in: .current))
                } else {
                    values.append(nil)
                }
            } else {
                values.append(nil)
            }
        }
        return values
    }
    
    public func execute(sql: String, params:[Any?]) throws {
        
        do {
            
            let statement = try db.prepareStatement(text: sql)
            try statement.execute(parameterValues: makeParams(params))
            
        }
        catch {
            print(error)
        }
        
    }
    
    fileprivate func migrate<T:SchemaVersioned>(iterator: ( (T) -> SchemaVersioned?)) {
        
        let fromInfo = T.version
        
        let sql = "SELECT value, partition, keyspace, id, ttl, filter FROM \(dataTableName) WHERE model = $1 AND version = $2 AND (ttl IS NULL or ttl >= $3)"
        
        let values = makeParams([fromInfo.objectName, fromInfo.version, ttl_now])
        
        do {
            
            //    let text = "SELECT city, temp_lo, temp_hi, prcp, date FROM weather WHERE city = $1;"
            //    let statement = try connection.prepareStatement(text: text)
            //    defer { statement.close() }
            //
            //    let cursor = try statement.execute(parameterValues: [ "San Francisco" ])
            //    defer { cursor.close() }
            //
            //    for row in cursor {
            //        let columns = try row.get().columns
            //        let city = try columns[0].string()
            //        let tempLo = try columns[1].int()
            //        let tempHi = try columns[2].int()
            //        let prcp = try columns[3].optionalDouble()
            //        let date = try columns[4].date()
            //
            //        print("""
            //            \(city) on \(date): low: \(tempLo), high: \(tempHi), \
            //            precipitation: \(String(describing: prcp))
            //            """)
            //    }
            
            let statement = try db.prepareStatement(text: sql)
            defer { statement.close() }
            
            let cursor = try statement.execute(parameterValues: values)
            defer { cursor.close() }

            // SELECT value, partition, keyspace, id, ttl, filter FROM
            //          0        1         2       3   4     5
            
            for r in cursor {
                let columns = try r.get().columns
                let partition = try columns[1].optionalString() ?? ""
                let keyspace = try columns[2].optionalString() ?? ""
                let id = try columns[3].optionalString() ?? ""
                var ttl: Int? = nil
                if let currentTTl = try columns[4].optionalInt() {
                    ttl = currentTTl - ttl_now
                }
                var filter: String? = nil
                if let currentFilter = try columns[5].optionalString() {
                    filter = currentFilter
                }
                if let bytea = try columns[0].optionalByteA() {
                    let d = bytea.data
                    if config.aes256encryptionKey == nil {
                        if let object = try? decoder.decode(T.self, from: Data(d)) {
                            if let newObject = iterator(object) {
                                if let filterable = newObject as? Filterable {
                                    var newFilter = ""
                                    for kvp in filterable.filters {
                                        let value = "\(kvp.key)=\(kvp.value)".md5()
                                        newFilter += " AND filter LIKE '%\(value)%' "
                                    }
                                    filter = newFilter
                                }
                                let _ = self.put(partition: partition, key: id, keyspace: keyspace, ttl: ttl ?? -1, filter: filter ?? "", newObject)
                            }
                        }
                    } else {
                        // this data is to be stored encrypted
                        if let encKey = config.aes256encryptionKey {
                            let key = encKey.sha256()
                            let iv = (encKey + Data(kSaltValue.bytes)).md5()
                            do {
                                let aes = try AES(key: key.bytes, blockMode: CBC(iv: iv.bytes))
                                let objectData = try aes.decrypt(bytea.data.bytes)
                                if let object = try? decoder.decode(T.self, from: Data(bytes: objectData, count: objectData.count)) {
                                    if let newObject = iterator(object) {
                                        if let filterable = newObject as? Filterable {
                                            var newFilter = ""
                                            for kvp in filterable.filters {
                                                let value = "\(kvp.key)=\(kvp.value)".md5()
                                                newFilter += " AND filter LIKE '%\(value)%' "
                                            }
                                            filter = newFilter
                                        }
                                        let _ = self.put(partition: partition, key: id, keyspace: keyspace, ttl: ttl ?? -1, filter: filter ?? "", newObject)
                                    }
                                }
                            } catch {
                                print("encryption error: \(error)")
                            }
                        }
                    }
                }
            }
            
        } catch {
            print(error)
        }
        
    }
    
    fileprivate func iterate<T:Codable>(sql: String, params:[Any?], iterator: ( (T) -> Void)) {
        
        let values = makeParams(params)
        
        do {
            
            let statement = try db.prepareStatement(text: sql)
            defer { statement.close() }
            
            let cursor = try statement.execute(parameterValues: values, retrieveColumnMetadata: true)
            defer { cursor.close() }

            // SELECT value, partition, keyspace, id, ttl, filter FROM
            //          0        1         2       3   4     5
            
            for r in cursor {
                if let columnMetaData = cursor.columns {
                    let columns = try r.get().columns
                    let index = columnMetaData.firstIndex(where: { $0.name == "value" }) ?? 0
                    if let d = try columns[index].optionalByteA()?.data {
                        if config.aes256encryptionKey == nil {
                            if let object = try? decoder.decode(T.self, from: Data(d.bytes)) {
                                iterator(object)
                            }
                        } else {
                            // this data is to be stored encrypted
                            if let encKey = config.aes256encryptionKey {
                                let key = encKey.sha256()
                                let iv = (encKey + Data(kSaltValue.bytes)).md5()
                                do {
                                    let aes = try AES(key: key.bytes, blockMode: CBC(iv: iv.bytes))
                                    let objectData = try aes.decrypt(d.bytes)
                                    if let object = try? decoder.decode(T.self, from: Data(bytes: objectData, count: objectData.count)) {
                                        iterator(object)
                                    }
                                } catch {
                                    print("encryption error: \(error)")
                                }
                            }
                        }
                    }
                }
            }
            
        } catch {
            print(error)
        }
        
    }
    
    public func query(sql: String, params:[Any?]) throws -> [[PostgresValue?]] {
        
        var results: [[PostgresValue?]] = []
        
        let values = makeParams(params)
        
        do {
            
            let statement = try db.prepareStatement(text: sql)
            defer { statement.close() }
            
            let cursor = try statement.execute(parameterValues: values, retrieveColumnMetadata: true)
            defer { cursor.close() }
      
            for r in cursor {
                var resultRow: [PostgresValue?] = []
                let columns = try r.get().columns
                for c in columns {
                    resultRow.append(c)
                }
                results.append(resultRow)
            }
            
        } catch {
            print(error)
            throw DatabaseError.Execute(.SyntaxError("\(error)"))
        }
        
        return results
        
    }
    
    public func transact(_ mode: transaction) -> Bool {
        return true
    }
    
    public func migrate<FromType: SchemaVersioned, ToType: SchemaVersioned>(from: FromType.Type, to: ToType.Type, migration: @escaping ((FromType) -> ToType?)) {
        self.migrate(iterator: migration)
    }
    
    /*
     *      insert into dummy(id, name, size) values(1, 'new_name', 3)
     on conflict(id)
     do update set name = 'new_name', size = 3;
     */
    
    
    
    public func put<T>(partition: String, key: String, keyspace: String, ttl: Int, filter: String, _ object: T) -> Bool where T : Decodable, T : Encodable {
        
        if let jsonObject = try? JSONEncoder().encode(object) {
            let id = makeId(key)
            do {
                if config.aes256encryptionKey == nil {
                    
                    var model: String? = nil
                    var version: Int? = nil
                    if let info = (T.self as? SchemaVersioned.Type)?.version {
                        model = info.objectName
                        version = info.version
                    }
                    
                    try execute(sql: "INSERT INTO \(dataTableName) (partition,keyspace,id,value,ttl,timestamp,model,version,filter) VALUES ($1,$2,$3,$4,$5,$6,$10,$11,$14) ON CONFLICT(partition,keyspace,id) DO UPDATE SET value = $7, ttl = $8, timestamp = $9, model = $12, version = $13, filter = $15;",
                                params: [
                                    partition,
                                    keyspace,
                                    id,
                                    jsonObject,ttl == -1 ? nil : Int(Date().timeIntervalSince1970) + ttl,
                                    Int(Date().timeIntervalSince1970),
                                    jsonObject,ttl == -1 ? nil : Int(Date().timeIntervalSince1970) + ttl,
                                    Int(Date().timeIntervalSince1970),
                                    model,
                                    version,
                                    model,
                                    version,
                                    filter,
                                    filter,
                                ])
                } else {
                    // this data is to be stored encrypted
                    if let encKey = config.aes256encryptionKey {
                        let key = encKey.sha256()
                        let iv = (encKey + Data(kSaltValue.bytes)).md5()
                        do {
                            let aes = try AES(key: key.bytes, blockMode: CBC(iv: iv.bytes))
                            let encryptedData = Data(try aes.encrypt(jsonObject.bytes))
                            
                            var model: String? = nil
                            var version: Int? = nil
                            if let info = (T.self as? SchemaVersioned.Type)?.version {
                                model = info.objectName
                                version = info.version
                            }
                            
                            try execute(sql: "INSERT INTO \(dataTableName) (partition,keyspace,id,value,ttl,timestamp,model,version, filter) VALUES ($1,$2,$3,$4,$5,$6,$10,$11,$14) ON CONFLICT(partition,keyspace,id) DO UPDATE SET value = $7, ttl = $8, timestamp = $9, model = $12, version = $13, filter = $15;",
                                        params: [
                                            partition,
                                            keyspace,
                                            id,
                                            encryptedData,ttl == -1 ? nil : Int(Date().timeIntervalSince1970) + ttl,
                                            Int(Date().timeIntervalSince1970),
                                            encryptedData,ttl == -1 ? nil : Int(Date().timeIntervalSince1970) + ttl,
                                            Int(Date().timeIntervalSince1970),
                                            model,
                                            version,
                                            model,
                                            version,
                                            filter,
                                            filter,
                                        ])
                        } catch {
                            print("encryption error: \(error)")
                        }
                    }
                }
                
                return true
            } catch {
                return false
            }
        }
        return false
    }
    
    
    public func delete(partition: String, key: String, keyspace: String) -> Bool {
        do {
            try execute(sql: "DELETE FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3;", params: [partition, keyspace, key])
            return true
        } catch {
            return false
        }
    }
    
    @discardableResult
    public func get<T>(partition: String, key: String, keyspace: String) -> T? where T : Decodable, T : Encodable {
        do {
            if config.aes256encryptionKey == nil {
                if let data = try query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3 AND (ttl IS NULL OR ttl >= $4)", params: [partition,keyspace,key,ttl_now]).first, let objectBytes = try? data[3]?.byteA().data {
                    let objectData = Data(objectBytes)
                    let object = try decoder.decode(T.self, from: objectData)
                    return object
                }
            } else {
                if let data = try query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3 AND (ttl IS NULL OR ttl >= $4)", params: [partition,keyspace,key,ttl_now]).first, let objectBytes = try? data[3]?.byteA().data, let encKey = config.aes256encryptionKey {
                    let objectData = Data(objectBytes)
                    let key = encKey.sha256()
                    let iv = (encKey + Data(kSaltValue.bytes)).md5()
                    do {
                        let aes = try AES(key: key.bytes, blockMode: CBC(iv: iv.bytes))
                        let decryptedBytes = try aes.decrypt(objectData.bytes)
                        let decryptedData = Data(decryptedBytes)
                        let object = try decoder.decode(T.self, from: decryptedData)
                        return object
                    } catch {
                        print("encryption error: \(error)")
                    }
                }
            }
        } catch {
            debugPrint("PostgresProvider Error:  Failed to decode stored object into type: \(T.self)")
            debugPrint("Error:")
            debugPrint(error)
            if let data = try? query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3 AND (ttl IS NULL OR ttl >= $4)", params: [partition,keyspace,key, ttl_now]).first, let objectBytes = try? data[3]?.byteA().data, let body = String(data: Data(objectBytes), encoding: .utf8) {
                let objectData = Data(objectBytes)
                debugPrint("Object data: \(String(data: objectData, encoding: .utf8) ?? "")")
                debugPrint(body)
                
            }
        }
        return nil
    }
    
    
    @discardableResult
    public func query<T>(partition: String, keyspace: String, filter: [String : String]?, map: ((T) -> Bool)) -> [T] where T : Decodable, T : Encodable {
        var results: [T] = []
        
        for result: T in all(partition: partition, keyspace: keyspace, filter: filter) {
            if map(result) {
                results.append(result)
            }
        }
        
        return results
    }
    
    @discardableResult
    public func all<T>(partition: String, keyspace: String, filter: [String : String]?) -> [T] where T : Decodable, T : Encodable {
        
        var f: String = ""
        if let filter = filter, filter.isEmpty == false {
            for kvp in filter {
                let value = "\(kvp.key)=\(kvp.value)".md5()
                f += " AND filter LIKE '%\(value)%' "
            }
        }
        
        do {
            let data = try query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND (ttl IS NULL OR ttl >= $3) \(f) ORDER BY timestamp ASC;", params: [partition, keyspace, ttl_now])
            var aggregation: [Data] = []
            for d in data.map({ try? $0[3]?.byteA().data }) {
                if config.aes256encryptionKey == nil {
                    if let d = d {
                        let objectData = Data(d)
                        aggregation.append(objectData)
                    }
                } else {
                    // this data is to be stored encrypted
                    if let encKey = config.aes256encryptionKey {
                        let key = encKey.sha256()
                        let iv = (encKey + Data(kSaltValue.bytes)).md5()
                        do {
                            let aes = try AES(key: key.bytes, blockMode: CBC(iv: iv.bytes))
                            if let d = d {
                                let encryptedData = Data(d)
                                let objectData = try aes.decrypt(encryptedData.bytes)
                                aggregation.append(Data(objectData))
                            }
                        } catch {
                            print("encryption error: \(error)")
                        }
                    }
                }
            }
            let opener = "[".data(using: .utf8)!
            let closer = "]".data(using: .utf8)!
            let separater = ",".data(using: .utf8)!
            var fullData = opener
            fullData.append(contentsOf: aggregation.joined(separator: separater))
            fullData.append(closer)
            if let results = try? JSONDecoder().decode([T].self, from: fullData) {
                return results
            } else {
                var results: [T] = []
                for v in aggregation {
                    if let object = try? JSONDecoder().decode(T.self, from: v) {
                        results.append(object)
                    }
                }
                return results
            }
        } catch  {
            return []
        }
    }
    
    public func iterate<T>(partition: String, keyspace: String, filter: [String : String]?, iterator: ((T) -> Void)) where T : Decodable, T : Encodable {
        
        var f: String = ""
        if let filter = filter, filter.isEmpty == false {
            for kvp in filter {
                let value = "\(kvp.key)=\(kvp.value)".md5()
                f += " AND filter LIKE '%\(value)%' "
            }
        }
        
        iterate(sql: "SELECT value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND (ttl IS NULL OR ttl >= $3) \(f) ORDER BY timestamp ASC;", params: [partition, keyspace, ttl_now], iterator: iterator)
        
    }
    
}
