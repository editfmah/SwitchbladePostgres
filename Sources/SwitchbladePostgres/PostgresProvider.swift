//
//  PostgresProvider.swift
//
//
//  Created by Adrian Herridge on 15/10/2021.
//

import Foundation

import Dispatch
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
    
    fileprivate var connectionString: String?
    fileprivate var host: String?
    fileprivate var port: Int = 5432
    fileprivate var username: String?
    fileprivate var password: String?
    fileprivate var database: String?
    fileprivate var connections: Int = 2
    fileprivate var ssl: Bool = false
    
    fileprivate var db: PostgresConnectionSource!
    
    let decoder: JSONDecoder = JSONDecoder()
    let encoder: JSONEncoder = JSONEncoder()
    
    var eventLoop: EventLoop { self.eventLoopGroup.next() }
    var eventLoopGroup: EventLoopGroup!
    var pool: EventLoopGroupConnectionPool<PostgresConnectionSource>!
    
    public init(connectionString: String)  {
        
        self.connectionString = connectionString
        
    }
    
    public init(host: String, username: String, password: String, database: String, connections: Int, ssl: Bool? = nil) {
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
        
        encoder.dataEncodingStrategy = .base64
        encoder.dateEncodingStrategy = .iso8601
        
        var configuration: PostgresConfiguration!
        if let connectionString = connectionString {
            configuration = PostgresConfiguration(url: connectionString)
            if configuration.tlsConfiguration != nil {
                configuration!.tlsConfiguration = .forClient(certificateVerification: .none)
            }
        } else if let host = host, let username = username, let password = password, let database = database {
            configuration = PostgresConfiguration(hostname: host, port: port, username: username, password: password, database: database)
            if ssl == false {
                configuration!.tlsConfiguration = nil
            } else {
                configuration!.tlsConfiguration = .forClient(certificateVerification: .none)
            }
            
        }
        
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: connections)
        
        self.db = PostgresConnectionSource(configuration: configuration!)
        self.pool = EventLoopGroupConnectionPool(
            source: db,
            maxConnectionsPerEventLoop: connections * 16,
            on: self.eventLoopGroup
        )
        
        try execute(sql: """
CREATE TABLE IF NOT EXISTS \(dataTableName) (
    partition text,
    keyspace text,
    id text,
    value json,
    ttl int,
    timestamp int,
    model text,
    version int,
    filter json,
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
    
    fileprivate func encodeValue(_ value: Any?) -> PostgresData {
        
        if value == nil {
            return PostgresData.null
        }
        
        if let value = value as? PostgresData {
            // already a postgres data object
            return value
        }
        
        // now go through the encodable types
        if let value = value as? Data {
            return PostgresData(bytes: value)
        } else if let value = value as? String {
            return PostgresData(string: value)
        } else if let value = value as? Int {
            return PostgresData(int: value)
        } else if let value = value as? Int64 {
            return PostgresData(int64: value)
        } else if let value = value as? UUID {
            return PostgresData(uuid: value)
        } else if let value = value as? Date {
            return PostgresData(date: value)
        } else if let value = value as? Encodable {
            return (try? PostgresData(json: value)) ?? PostgresData.null
        }
        
        return PostgresData.null
    }
    
    fileprivate func makeParams(_ params:[Any?]) -> [PostgresData] {
        var values: [PostgresData] = []
        for p in params {
            values.append(encodeValue(p))
        }
        return values
    }
    
    public func execute(sql: String, params:[Any?]) throws {
        
        let values = makeParams(params)
        
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
        
        let sql = "SELECT value, partition, keyspace, id, ttl FROM \(dataTableName) WHERE model = $1 AND version = $2 AND (ttl IS NULL or ttl >= $3)"
        
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
                
                if let d = r.column("value")?.json {
                    if let object = try? decoder.decode(T.self, from: Data(d)) {
                        if let newObject = iterator(object) {
                            let _ = self.put(
                                partition: partition,
                                key: id,
                                keyspace: keyspace,
                                ttl: ttl ?? -1,
                                filter: (newObject as? Filterable)?.filters.dictionary ?? [:],
                                newObject
                            )
                        }
                    }
                }
            }
            
        } catch {
            print(error)
        }
        
    }
    
    fileprivate func iterate<T:Codable>(sql: String, params:[Any?], iterator: ( (T) -> Void)) {
        
        var values = makeParams(params)
        
        do {
            
            let rows = try pool.withConnection { conn in
                return conn.query(sql, values)
            }.wait()
            
            for r in rows {
                if let d = r.column("value")?.json {
                    if let object = try? decoder.decode(T.self, from: Data(d)) {
                        iterator(object)
                    }
                }
            }
            
        } catch {
            print(error)
        }
        
    }
    
    public func query(sql: String, params:[Any?]) throws -> [[PostgresData?]] {
        
        var results: [[PostgresData?]] = []
        
        var values: [PostgresData] = []
        for p in params {
            values.append(encodeValue(p))
        }
        
        do {
            
            let rows = try pool.withConnection { conn in
                return conn.query(sql, values)
            }.wait()
            
            var columnNames: [String] = []
            if let firstRow = rows.first {
                columnNames = firstRow.makeRandomAccess().compactMap({ $0.columnName })
            }
            
            for r in rows {
                var resultRow: [PostgresData?] = []
                for c in columnNames {
                    resultRow.append(r.column(c))
                }
                results.append(resultRow)
            }
            
        } catch {
            print(error)
            throw DatabaseError.Execute(.SyntaxError("\(error)"))
        }
        
        return results
        
    }
    
    public func ids(partition: String, keyspace: String, filter: [String : String]?) -> [String] {
        
        var f: String = ""
        if let filter = filter, filter.isEmpty == false {
            for kvp in filter {
                f += " AND filter->>'\(kvp.key)' = '\(kvp.value)' "
            }
        }
        
        do {
            let data = try query(sql: "SELECT id FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND (ttl IS NULL OR ttl >= $3) \(f) ORDER BY timestamp ASC;", params: [partition, keyspace, ttl_now])
            return data.map({ $0[0]?.string ?? "" })
        } catch {
            return []
        }
    }
    
    public func transact(_ mode: transaction) -> Bool {
        return true
    }
    
    public func migrate<FromType: SchemaVersioned, ToType: SchemaVersioned>(from: FromType.Type, to: ToType.Type, migration: @escaping ((FromType) -> ToType?)) {
        self.migrate(iterator: migration)
    }
    
    public func put<T>(partition: String, key: String, keyspace: String, ttl: Int, filter: [String : String]?, _ object: T) -> Bool where T : Decodable, T : Encodable {
        
        
        let id = makeId(key)
        do {
            
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
                            PostgresData(json: object),
                            ttl == -1 ? nil : (Int(Date().timeIntervalSince1970) + ttl),
                            Int(Date().timeIntervalSince1970),
                            PostgresData(json: object),
                            ttl == -1 ? nil : (Int(Date().timeIntervalSince1970) + ttl),
                            Int(Date().timeIntervalSince1970),
                            model,
                            version,
                            model,
                            version,
                            filter,
                            filter,
                        ])
            
            return true
        } catch {
            return false
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
            if let data = try query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3 AND (ttl IS NULL OR ttl >= $4)", params: [partition,keyspace,key,ttl_now]).first, let objectBytes = data[3]?.json {
                let objectData = Data(objectBytes)
                let object = try decoder.decode(T.self, from: objectData)
                return object
            }
        } catch {
            debugPrint("PostgresProvider Error:  Failed to decode stored object into type: \(T.self)")
            debugPrint("Error:")
            debugPrint(error)
            if let data = try? query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3 AND (ttl IS NULL OR ttl >= $4)", params: [partition,keyspace,key, ttl_now]).first, let objectBytes = data[3]?.json, let body = String(data: Data(objectBytes), encoding: .utf8) {
                let objectData = Data(objectBytes)
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
                f += " AND filter->>'\(kvp.key)' = '\(kvp.value)' "
            }
        }
        
        do {
            var results: [T] = []
            let data = try query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND (ttl IS NULL OR ttl >= $3) \(f) ORDER BY timestamp ASC;", params: [partition, keyspace, ttl_now])
            for d in data.compactMap({ try? $0[3]?.json(as: T.self) }) {
                results.append(d)
            }
            return results
        } catch  {
            return []
        }
    }
    
    public func iterate<T>(partition: String, keyspace: String, filter: [String : String]?, iterator: ((T) -> Void)) where T : Decodable, T : Encodable {
        
        var f: String = ""
        if let filter = filter, filter.isEmpty == false {
            for kvp in filter {
                f += " AND filter->>'\(kvp.key)' = '\(kvp.value)' "
            }
        }
        
        iterate(sql: "SELECT value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND (ttl IS NULL OR ttl >= $3) \(f) ORDER BY timestamp ASC;", params: [partition, keyspace, ttl_now], iterator: iterator)
        
    }
    
}
