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

/// Configuration for retry behavior
public struct RetryConfiguration {
    public let maxAttempts: Int
    public let baseDelayMs: UInt64
    public let maxDelayMs: UInt64
    public let jitterFactor: Double

    public static let `default` = RetryConfiguration(
        maxAttempts: 5,
        baseDelayMs: 50,
        maxDelayMs: 2000,
        jitterFactor: 0.25
    )

    public init(maxAttempts: Int, baseDelayMs: UInt64, maxDelayMs: UInt64, jitterFactor: Double) {
        self.maxAttempts = maxAttempts
        self.baseDelayMs = baseDelayMs
        self.maxDelayMs = maxDelayMs
        self.jitterFactor = jitterFactor
    }
}

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
    fileprivate var connections: Int
    fileprivate var ssl: Bool = false

    public var retryConfig: RetryConfiguration

    private let lock = NSLock()
    private var isOpen = false
    private var ttlCleanupWorkItem: DispatchWorkItem?

    fileprivate var db: PostgresConnectionSource!

    private let decoder: JSONDecoder = {
        let d = JSONDecoder()
        d.dataDecodingStrategy = .base64
        d.dateDecodingStrategy = .iso8601
        return d
    }()

    private let encoder: JSONEncoder = {
        let e = JSONEncoder()
        e.dataEncodingStrategy = .base64
        e.dateEncodingStrategy = .iso8601
        return e
    }()

    var eventLoop: EventLoop { self.eventLoopGroup.next() }
    var eventLoopGroup: EventLoopGroup!
    var pool: EventLoopGroupConnectionPool<PostgresConnectionSource>!

    public init(connectionString: String, connections: Int = 8, retryConfig: RetryConfiguration = .default)  {
        self.connectionString = connectionString
        self.connections = max(1, connections)
        self.retryConfig = retryConfig
    }

    public init(host: String, username: String, password: String, database: String, connections: Int = 8, ssl: Bool? = nil, retryConfig: RetryConfiguration = .default) {
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.connections = max(1, connections)
        self.retryConfig = retryConfig
        if let ssl = ssl {
            self.ssl = ssl
        }
    }

    public func open() throws {
        lock.lock()
        defer { lock.unlock() }

        guard !isOpen else { return }

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
            maxConnectionsPerEventLoop: 4,
            on: self.eventLoopGroup
        )

        isOpen = true

        try executeWithRetry(sql: """
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

        try executeWithRetry(sql: "CREATE INDEX IF NOT EXISTS idx_ttl ON \(dataTableName) (ttl);", params: [])
        try executeWithRetry(sql: "CREATE INDEX IF NOT EXISTS idx_model ON \(dataTableName) (model,version);", params: [])

        startTTLCleanup()
    }

    private func startTTLCleanup() {
        let workItem = DispatchWorkItem { [weak self] in
            self?.ttlCleanupLoop()
        }
        ttlCleanupWorkItem = workItem
        DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + 60, execute: workItem)
    }

    private func ttlCleanupLoop() {
        guard isOpen else { return }

        try? executeWithRetry(sql: "DELETE FROM \(dataTableName) WHERE ttl IS NOT NULL AND ttl < $1;", params: [ttl_now])

        let workItem = DispatchWorkItem { [weak self] in
            self?.ttlCleanupLoop()
        }
        ttlCleanupWorkItem = workItem
        DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + 60, execute: workItem)
    }

    public func close() throws {
        lock.lock()
        defer { lock.unlock() }

        guard isOpen else { return }
        isOpen = false

        ttlCleanupWorkItem?.cancel()
        ttlCleanupWorkItem = nil

        pool.shutdown()
        try? eventLoopGroup.syncShutdownGracefully()
        db = nil
    }

    fileprivate func makeId(_ key: String) -> String {
        return key
    }

    fileprivate func encodeValue(_ value: Any?) -> PostgresData {

        if value == nil {
            return PostgresData.null
        }

        if let value = value as? PostgresData {
            return value
        }

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

    private func calculateDelay(attempt: Int) -> UInt64 {
        let exponentialDelay = retryConfig.baseDelayMs * (1 << UInt64(attempt))
        let cappedDelay = min(exponentialDelay, retryConfig.maxDelayMs)
        let jitter = Double(cappedDelay) * retryConfig.jitterFactor * Double.random(in: -1...1)
        return UInt64(max(0, Double(cappedDelay) + jitter))
    }

    private func shouldRetry(error: Error) -> Bool {
        let errorString = "\(error)".lowercased()

        let retryablePatterns = [
            "connection reset",
            "connection refused",
            "connection closed",
            "broken pipe",
            "timed out",
            "timeout",
            "too many connections",
            "server closed",
            "connection lost",
            "connection terminated",
            "eof",
            "network",
            "socket",
            "could not connect",
            "temporarily unavailable",
            "try again",
            "deadlock",
            "serialization failure",
            "40001",
            "40p01",
            "53300",
            "57p01",
            "08006",
            "08003",
            "08001",
        ]

        for pattern in retryablePatterns {
            if errorString.contains(pattern) {
                return true
            }
        }

        return false
    }

    private func sleepMs(_ ms: UInt64) {
        Thread.sleep(forTimeInterval: Double(ms) / 1000.0)
    }

    fileprivate func executeWithRetry(sql: String, params: [Any?]) throws {
        let values = makeParams(params)
        var lastError: Error?

        for attempt in 0..<retryConfig.maxAttempts {
            do {
                _ = try pool.withConnection { conn in
                    return conn.query(sql, values)
                }.wait()
                return
            } catch {
                lastError = error

                if attempt < retryConfig.maxAttempts - 1 && shouldRetry(error: error) {
                    let delay = calculateDelay(attempt: attempt)
                    sleepMs(delay)
                } else if !shouldRetry(error: error) {
                    break
                }
            }
        }

        if let error = lastError {
            throw DatabaseError.Execute(.SyntaxError("\(error)"))
        }
    }

    fileprivate func queryWithRetry(sql: String, params: [Any?]) throws -> [PostgresRow] {
        let values = makeParams(params)
        var lastError: Error?

        for attempt in 0..<retryConfig.maxAttempts {
            do {
                let result = try pool.withConnection { conn in
                    return conn.query(sql, values)
                }.wait()
                return result.rows
            } catch {
                lastError = error

                if attempt < retryConfig.maxAttempts - 1 && shouldRetry(error: error) {
                    let delay = calculateDelay(attempt: attempt)
                    sleepMs(delay)
                } else if !shouldRetry(error: error) {
                    break
                }
            }
        }

        throw lastError ?? DatabaseError.Execute(.SyntaxError("Unknown error"))
    }

    public func execute(sql: String, params:[Any?]) throws {
        try executeWithRetry(sql: sql, params: params)
    }

    fileprivate func migrate<T:SchemaVersioned>(iterator: ( (T) -> SchemaVersioned?)) {

        let fromInfo = T.version

        let sql = "SELECT value, partition, keyspace, id, ttl FROM \(dataTableName) WHERE model = $1 AND version = $2 AND (ttl IS NULL or ttl >= $3)"

        do {

            let rows = try queryWithRetry(sql: sql, params: [fromInfo.objectName, fromInfo.version, ttl_now])

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
            debugPrint("Migration error: \(error)")
        }

    }

    fileprivate func iterate<T:Codable>(sql: String, params:[Any?], iterator: ( (T) -> Void)) {

        do {
            let rows = try queryWithRetry(sql: sql, params: params)

            for r in rows {
                if let d = r.column("value")?.json {
                    if let object = try? decoder.decode(T.self, from: Data(d)) {
                        iterator(object)
                    }
                }
            }

        } catch {
            debugPrint("Iterate error: \(error)")
        }

    }

    public func query(sql: String, params:[Any?]) throws -> [[PostgresData?]] {

        var results: [[PostgresData?]] = []

        let rows = try queryWithRetry(sql: sql, params: params)

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

        var model: String? = nil
        var version: Int? = nil
        if let info = (T.self as? SchemaVersioned.Type)?.version {
            model = info.objectName
            version = info.version
        }

        var lastError: Error?

        for attempt in 0..<retryConfig.maxAttempts {
            do {
                try executeWithRetry(sql: "INSERT INTO \(dataTableName) (partition,keyspace,id,value,ttl,timestamp,model,version,filter) VALUES ($1,$2,$3,$4,$5,$6,$10,$11,$14) ON CONFLICT(partition,keyspace,id) DO UPDATE SET value = $7, ttl = $8, timestamp = $9, model = $12, version = $13, filter = $15;",
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
                lastError = error
                if attempt < retryConfig.maxAttempts - 1 {
                    let delay = calculateDelay(attempt: attempt)
                    sleepMs(delay)
                }
            }
        }

        debugPrint("Put failed after \(retryConfig.maxAttempts) attempts: \(lastError?.localizedDescription ?? "unknown")")
        return false
    }


    public func delete(partition: String, key: String, keyspace: String) -> Bool {
        var lastError: Error?

        for attempt in 0..<retryConfig.maxAttempts {
            do {
                try executeWithRetry(sql: "DELETE FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3;", params: [partition, keyspace, key])
                return true
            } catch {
                lastError = error
                if attempt < retryConfig.maxAttempts - 1 {
                    let delay = calculateDelay(attempt: attempt)
                    sleepMs(delay)
                }
            }
        }

        debugPrint("Delete failed after \(retryConfig.maxAttempts) attempts: \(lastError?.localizedDescription ?? "unknown")")
        return false
    }

    @discardableResult
    public func get<T>(partition: String, key: String, keyspace: String) -> T? where T : Decodable, T : Encodable {
        var lastError: Error?

        for attempt in 0..<retryConfig.maxAttempts {
            do {
                if let data = try query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND id = $3 AND (ttl IS NULL OR ttl >= $4)", params: [partition,keyspace,key,ttl_now]).first, let objectBytes = data[3]?.json {
                    let objectData = Data(objectBytes)
                    let object = try decoder.decode(T.self, from: objectData)
                    return object
                }
                return nil
            } catch let error where shouldRetry(error: error) {
                lastError = error
                if attempt < retryConfig.maxAttempts - 1 {
                    let delay = calculateDelay(attempt: attempt)
                    sleepMs(delay)
                }
            } catch {
                debugPrint("PostgresProvider Error: Failed to decode stored object into type: \(T.self)")
                debugPrint("Error: \(error)")
                return nil
            }
        }

        debugPrint("Get failed after \(retryConfig.maxAttempts) attempts: \(lastError?.localizedDescription ?? "unknown")")
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

        var lastError: Error?

        for attempt in 0..<retryConfig.maxAttempts {
            do {
                var results: [T] = []
                let data = try query(sql: "SELECT partition,keyspace,id,value FROM \(dataTableName) WHERE partition = $1 AND keyspace = $2 AND (ttl IS NULL OR ttl >= $3) \(f) ORDER BY timestamp ASC;", params: [partition, keyspace, ttl_now])
                for d in data.compactMap({ try? $0[3]?.json(as: T.self) }) {
                    results.append(d)
                }
                return results
            } catch let error where shouldRetry(error: error) {
                lastError = error
                if attempt < retryConfig.maxAttempts - 1 {
                    let delay = calculateDelay(attempt: attempt)
                    sleepMs(delay)
                }
            } catch {
                return []
            }
        }

        debugPrint("All failed after \(retryConfig.maxAttempts) attempts: \(lastError?.localizedDescription ?? "unknown")")
        return []
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
