//
//  PostgresProvider.swift
//  
//
//  Created by Adrian Herridge on 15/10/2021.
//

import Foundation

import Dispatch
import CryptoSwift
import PostgresKit
import Switchblade

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
    
    fileprivate var connectionString: String!
    
    fileprivate var db: PostgresConnectionSource!
    
    let decoder: JSONDecoder = JSONDecoder()
    
    var eventLoop: EventLoop { self.eventLoopGroup.next() }
    var eventLoopGroup: EventLoopGroup!
    var pool: EventLoopGroupConnectionPool<PostgresConnectionSource>!
    
    public init(connectionString: String)  {
        
        self.connectionString = connectionString
        
    }
    
    public func open() throws {
        
        var configuration = PostgresConfiguration(url: self.connectionString)
        configuration!.tlsConfiguration = .forClient(certificateVerification: .none)
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        
        self.db = PostgresConnectionSource(configuration: configuration!)
        self.pool = EventLoopGroupConnectionPool(
            source: db,
            maxConnectionsPerEventLoop: 2,
            on: self.eventLoopGroup
        )
        
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
        pool.shutdown()
    }
    
    fileprivate func makeId(_ key: String) -> String {
        return key
    }
    
    public func execute(sql: String, params:[Any?]) throws {
        
        var values: [PostgresData] = []
        for p in params {
            if let p = p {
                if let value = p as? Data {
                    values.append(PostgresData(bytes: value))
                } else if let value = p as? String {
                    values.append(PostgresData(string: value))
                } else if let value = p as? Int {
                    values.append(PostgresData(int: value))
                } else {
                    values.append(PostgresData.null)
                }
            } else {
                values.append(PostgresData.null)
            }
        }
        
        do {
            
            _ = try pool.withConnection { conn in
                return conn.query(sql, values)
            }.wait()
            
        } catch {
            print(error)
            throw DatabaseError.Execute(.SyntaxError("\(error)"))
        }
        
    }
    
    fileprivate func migrate<T:SchemaVersioned>(iterator: ( (T) -> SchemaVersioned?)) {
        
        let fromInfo = T.version

        let sql = "SELECT value, partition, keyspace, id, ttl, filter FROM \(dataTableName) WHERE model = $1 AND version = $2 AND (ttl IS NULL or ttl >= $3)"
        
        var values: [PostgresData] = [PostgresData(string: fromInfo.objectName), PostgresData(int: fromInfo.version), PostgresData(int: ttl_now)]
        
        do {
            
            let rows = try pool.withConnection { conn in
                return conn.query(sql, values)
            }.wait()
            
            for r in rows {
                let partition = r.column("partition")?.string ?? ""
                let keyspace = r.column("keyspace")?.string ?? ""
                let id = r.column("id")?.string ?? ""
                var ttl: Int? = nil
                if let currentTTl = r.column("ttl")?.int {
                    ttl = currentTTl - ttl_now
                }
                var filter: String? = nil
                if let currentFilter = r.column("filter")?.string {
                    filter = currentFilter
                }
                if let d = r.column("value")?.bytes {
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
                                let objectData = try aes.decrypt(d)
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
        
        var values: [PostgresData] = []
        for p in params {
            if let p = p {
                if let value = p as? Data {
                    values.append(PostgresData(bytes: value))
                } else if let value = p as? String {
                    values.append(PostgresData(string: value))
                } else if let value = p as? Int {
                    values.append(PostgresData(int: value))
                } else {
                    values.append(PostgresData.null)
                }
            } else {
                values.append(PostgresData.null)
            }
        }
        
        do {
            
            let rows = try pool.withConnection { conn in
                return conn.query(sql, values)
            }.wait()
            
            for r in rows {
                if let d = r.column("value")?.bytes {
                    if config.aes256encryptionKey == nil {
                        if let object = try? decoder.decode(T.self, from: Data(d)) {
                            iterator(object)
                        }
                    } else {
                        // this data is to be stored encrypted
                        if let encKey = config.aes256encryptionKey {
                            let key = encKey.sha256()
                            let iv = (encKey + Data(kSaltValue.bytes)).md5()
                            do {
                                let aes = try AES(key: key.bytes, blockMode: CBC(iv: iv.bytes))
                                let objectData = try aes.decrypt(d)
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
            
        } catch {
            print(error)
        }
        
    }
    
    public func query(sql: String, params:[Any?]) throws -> [(partition: String, keyspace: String, id: String, value: Data?)] {
        
        var results: [(partition: String, keyspace: String, id: String, value: Data?)] = []
        
        var values: [PostgresData] = []
        for p in params {
            if let p = p {
                if let value = p as? Data {
                    values.append(PostgresData(bytes: value))
                } else if let value = p as? String {
                    values.append(PostgresData(string: value))
                } else if let value = p as? Int {
                    values.append(PostgresData(int: value))
                } else {
                    values.append(PostgresData.null)
                }
            } else {
                values.append(PostgresData.null)
            }
        }
        
        do {
            
            let rows = try pool.withConnection { conn in
                return conn.query(sql, values)
            }.wait()
            
            for r in rows {
                let p = r.column("partition")?.string ?? ""
                let k = r.column("keyspace")?.string ?? ""
                let id = r.column("id")?.string ?? ""
                let val = Data(r.column("value")?.bytes ?? [])
                results.append((partition: p, keyspace: k, id: id, value: val))
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
                if let data = try query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3 AND (ttl IS NULL OR ttl >= $4)", params: [partition,keyspace,key,ttl_now]).first, let objectData = data.value {
                    let object = try decoder.decode(T.self, from: objectData)
                    return object
                }
            } else {
                if let data = try query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3 AND (ttl IS NULL OR ttl >= $4)", params: [partition,keyspace,key,ttl_now]).first, let objectData = data.value, let encKey = config.aes256encryptionKey {
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
            debugPrint("SQLiteProvider Error:  Failed to decode stored object into type: \(T.self)")
            debugPrint("Error:")
            debugPrint(error)
            if let data = try? query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3 AND (ttl IS NULL OR ttl >= $4)", params: [partition,keyspace,key, ttl_now]).first, let objectData = data.value, let body = String(data: objectData, encoding: .utf8) {
                
                debugPrint("Object data:")
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
            for d in data.map({ $0.value }) {
                if config.aes256encryptionKey == nil {
                    if let objectData = d {
                        aggregation.append(objectData)
                    }
                } else {
                    // this data is to be stored encrypted
                    if let encKey = config.aes256encryptionKey {
                        let key = encKey.sha256()
                        let iv = (encKey + Data(kSaltValue.bytes)).md5()
                        do {
                            let aes = try AES(key: key.bytes, blockMode: CBC(iv: iv.bytes))
                            if let encryptedData = d {
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
