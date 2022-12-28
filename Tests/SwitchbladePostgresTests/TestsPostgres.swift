//
//  File.swift
//  
//
//  Created by Adrian Herridge on 21/05/2022.
//

import Foundation
import XCTest
@testable import Switchblade
@testable import SwitchbladePostgres

let testLock = Mutex()
var db: Switchblade!

public class Person : Codable, Identifiable, KeyspaceIdentifiable {
    
    public var key: PrimaryKeyType {
        return self.PersonId
    }
    
    public var keyspace: String {
        return "person"
    }
    
    init(){ PersonId = UUID() }
    public var PersonId : UUID
    public var Name: String?
    public var Age: Int?
    public var DepartmentId : UUID?
    
}

public class PersonVersion1 : Codable, SchemaVersioned {
    
    public static var version: (objectName: String, version: Int) = ("Person", 1)
    
    
    public var id : UUID = UUID()
    public var name: String?
    public var age: Int?
    
}

public class PersonVersion2 : Codable, SchemaVersioned {
    
    public static var version: (objectName: String, version: Int) = ("Person", 2)
    
    public var id : UUID = UUID()
    public var forename: String?
    public var surname: String?
    public var age: Int?
    
}

public class PersonFilterable : Codable, Filterable, Identifiable, KeyspaceIdentifiable {
    
    public var filters: [String : String] {
        get {
            return ["type" : "person", "age" : "\(self.Age ?? 0)", "name" : self.Name ?? ""]
        }
    }
    
    public var key: PrimaryKeyType {
        return self.PersonId
    }
    
    public var keyspace: String {
        return "person"
    }
    
    init(){ PersonId = UUID() }
    public var PersonId : UUID
    public var Name: String?
    public var Age: Int?
    public var DepartmentId : UUID?
    
}

func initPostgresDatabase(_ config: SwitchbladeConfig? = nil) -> Switchblade {
    
    if db == nil {
        
        let config = SwitchbladeConfig()
        
        db = try! Switchblade(provider: PostgresProvider(connectionString: ""))
        
        let provider = db.provider as? PostgresProvider
        try? provider?.execute(sql: "DELETE FROM Data;", params: [])
        
    }

    return db
    
}

extension SwitchbladePostgresTests {
    
    func testPersistObject() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 40
        if db.put(partition: partition, ttl: nil, p1) {
            p2.Name = "Neil Bostrom"
            p2.Age = 37
            if db.put(partition: partition, ttl: nil,p2) {
                p3.Name = "George Smith"
                p3.Age = 28
                if db.put(partition: partition, ttl: nil,p3) {
                    return
                }
            }
        }
        
        XCTFail("failed to write one of the records")
        
    }
    
    func testPersistQueryObject() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 40
        if db.put(partition: partition, ttl: nil,p1) {
            if let retrieved: Person = db.get(partition: partition, key: p1.key, keyspace: p1.keyspace) {
                print("retrieved item with id \(retrieved.PersonId)")
                return
            } else {
                XCTFail("failed to retrieve object")
            }
        }
        
        XCTFail("failed to write one of the records")
        
    }
    
    func testPersistSingleObjectAndCheckAll() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        if db.put(partition: partition, ttl: nil,p1) {
            let results: [Person] = db.all(partition: partition, keyspace: p1.keyspace)
            if results.count == 1 {
                return
            } else {
                XCTFail("failed to read back the correct number of records")
            }
        }
        XCTFail("failed to write one of the records")
    }

    
    func testPersistMultipleObjectsAndCheckAll() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        if db.put(partition: partition, ttl: nil,p1) {
            p2.Name = "Neil Bostrom"
            p2.Age = 38
            if db.put(partition: partition, ttl: nil,p2) {
                p3.Name = "George Smith"
                p3.Age = 28
                if db.put(partition: partition, ttl: nil,p3) {
                    let results: [Person] = db.all(partition: partition, keyspace: p1.keyspace)
                    if results.count == 3 {
                        return
                    } else {
                        XCTFail("failed to read back the correct number of records")
                    }
                }
            }
        }
        XCTFail("failed to write one of the records")
    }
    
    func testPersistMultipleObjectsAndFilterAll() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        if db.put(partition: partition, ttl: nil,p1) {
            p2.Name = "Neil Bostrom"
            p2.Age = 38
            if db.put(partition: partition, ttl: nil,p2) {
                p3.Name = "George Smith"
                p3.Age = 28
                if db.put(partition: partition, ttl: nil,p3) {
                    if let _ : Person = db.all(partition: partition, keyspace: p1.keyspace).first(where: { $0.Age == 41 && $0.Name == "Adrian Herridge" }) {
                        return
                    } else {
                        XCTFail("failed to read back the correct records")
                    }
                }
            }
        }
        XCTFail("failed to write one of the records")
    }
    
    func testPersistMultipleObjectsAndQuery() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        if db.put(partition: partition, ttl: nil,p1) {
            p2.Name = "Neil Bostrom"
            p2.Age = 38
            if db.put(partition: partition, ttl: nil,p2) {
                p3.Name = "George Smith"
                p3.Age = 28
                if db.put(partition: partition, ttl: nil,p3) {
                    let results: [Person] = db.query(partition: partition, keyspace: p1.keyspace) { person in
                        return person.Age == 41
                    }
                    if results.count == 1 {
                        if let result = results.first, result.Name == "Adrian Herridge" {
                            return
                        } else {
                            XCTFail("failed to read back the correct record")
                        }
                    } else {
                        XCTFail("failed to read back the correct record")
                    }
                }
            }
        }
        XCTFail("failed to write one of the records")
    }
    
    func testPersistMultipleObjectsAndQueryMultipleParams() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        if db.put(partition: partition, ttl: nil,p1) {
            p2.Name = "Neil Bostrom"
            p2.Age = 41
            if db.put(partition: partition, ttl: nil,p2) {
                p3.Name = "George Smith"
                p3.Age = 28
                if db.put(partition: partition, ttl: nil,p3) {
                    let results: [Person] = db.query(partition: partition, keyspace: p1.keyspace) { result in
                        return result.Age == 41 && result.Name == "Adrian Herridge"
                    }
                    if results.count == 1 {
                        if let result = results.first, result.Name == "Adrian Herridge" {
                            return
                        } else {
                            XCTFail("failed to read back the correct record")
                        }
                    } else {
                        XCTFail("failed to read back the correct record")
                    }
                }
            }
        }
        XCTFail("failed to write one of the records")
    }
    
    func testPersistAndQueryObjectEncrypted() {
        
        let config = SwitchbladeConfig()
        config.aes256encryptionKey = Data("big_sprouts".utf8)
        let db = initPostgresDatabase(config)
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        if db.put(partition: partition, ttl: nil,p1) {
            if let _: Person = db.get(partition: partition, key: p1.key, keyspace: p1.keyspace) {
                
            } else {
                XCTFail("failed to retrieve one of the records")
            }
        } else {
            XCTFail("failed to write one of the records")
        }
    }
    
    func testPersistAndQueryObjectEncryptedWrongSeed() {
        
        let config = SwitchbladeConfig()
        config.aes256encryptionKey = Data("big_sprouts".utf8)
        let db = initPostgresDatabase(config)
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        if db.put(partition: partition, ttl: nil,p1) {
            config.aes256encryptionKey = Data("small_sprouts".utf8)
            if let retrieved: Person = db.get(partition: partition, key: p1.key, keyspace: p1.keyspace) {
                XCTFail("failed to retrieve one of the records")
            } else {
                
            }
        } else {
            XCTFail("failed to write one of the records")
        }
    }
    
    func testPersistAndQueryObjectPropertiesEncrypted() {
        
        let config = SwitchbladeConfig()
        config.aes256encryptionKey = Data("big_sprouts".utf8)
        config.hashQueriableProperties = true
        let db = initPostgresDatabase(config)
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        if db.put(partition: partition, ttl: nil,p1) {
            let retrieved: [Person] = db.query(partition: partition, keyspace: p1.keyspace) { result in
                return result.Age == 41
            }
            if retrieved.count == 1 {
                
            } else {
                XCTFail("failed to retrieve one of the records")
            }
        } else {
            XCTFail("failed to write one of the records")
        }
    }
    
    func testQueryParamEqualls() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        db.put(partition: partition, ttl: nil,p1)
        p2.Name = "Neil Bostrom"
        p2.Age = 38
        db.put(partition: partition, ttl: nil,p2)
        p3.Name = "George Smith"
        p3.Age = 28
        db.put(partition: partition, ttl: nil,p3)
        
        let results: [Person] = db.query(partition: partition, keyspace: p1.keyspace) { result in
            return result.Age == 41
        }
        if results.count == 1 {
            if let result = results.first, result.Name == "Adrian Herridge" {
                return
            }
        }
        
        XCTFail("failed to write one of the recordss")
    }
    
    func testQueryParamGreaterThan() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        db.put(partition: partition, ttl: nil,p1)
        p2.Name = "Neil Bostrom"
        p2.Age = 38
        db.put(partition: partition, ttl: nil,p2)
        p3.Name = "George Smith"
        p3.Age = 28
        db.put(partition: partition, ttl: nil,p3)
        
        let results: [Person] = db.query(partition: partition, keyspace: p1.keyspace) { p in
            return p.Age ?? 0 > 30
        }
        if results.count == 2 {
            return
        }
        
        XCTFail("failed to write one of the recordss")
    }
    
    func testQueryParamLessThan() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        db.put(partition: partition, ttl: nil,p1)
        p2.Name = "Neil Bostrom"
        p2.Age = 38
        db.put(partition: partition, ttl: nil,p2)
        p3.Name = "George Smith"
        p3.Age = 28
        db.put(partition: partition, ttl: nil,p3)
        
        let results: [Person] = db.query(partition: partition, keyspace: p1.keyspace) { p in
            if let age = p.Age {
                return age < 40
            }
            return false
        }
        if results.count == 2 {
            return
        }
        
        XCTFail("failed to write one of the recordss")
    }
    
    func testQueryParamIsNull() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        db.put(partition: partition, ttl: nil,p1)
        p2.Name = "Neil Bostrom"
        p2.Age = nil
        db.put(partition: partition, ttl: nil,p2)
        p3.Name = "George Smith"
        p3.Age = 28
        db.put(partition: partition, ttl: nil,p3)
        
        let results: [Person] = db.query(partition: partition, keyspace: p1.keyspace) { p in
            return p.Name == "Neil Bostrom" && p.Age == nil
        }
        if results.count == 1 {
            if let result = results.first, result.Name == "Neil Bostrom" {
                return
            }
        }
        
        XCTFail("failed to write one of the recordss")
    }
    
    
    func testReadWriteFromUserDefaults() {
        
        let db = try! Switchblade(provider: UserDefaultsProvider())
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        p1.Name = "Sunjay Kalsi"
        p1.Age = 43
        db.put(partition: partition, ttl: nil,p1)
        
        if let _: Person = db.get(partition: partition, key: p1.key, keyspace: p1.keyspace) {
            
        } else {
            XCTFail("failed to get object from provider")
        }
        
        let db2 = try! Switchblade(provider: UserDefaultsProvider())
        if let p2: Person = db2.get(key: p1.key, keyspace: p1.keyspace) {
            print("retrieved record for '\(p2.Name ?? "")'")
        } else {
            XCTFail("failed to get object from provider")
        }
        
    }
    
    func testPersistObjectCompositeKey() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 40
        if db.put(partition: partition,compositeKeys: ["ad",1,"testing123"], ttl: nil,p1) {
            p2.Name = "Neil Bostrom"
            p2.Age = 37
            if db.put(partition: partition,compositeKeys:["bozzer",2,"testing123"],ttl: nil,p2) {
                p3.Name = "George Smith"
                p3.Age = 28
                if db.put(partition: partition,compositeKeys:["george",3,"testing123"], ttl: nil, p3) {
                    return
                }
            }
        }
        
        XCTFail("failed to write one of the records")
        
    }
    
    func testPersistQueryObjectCompositeKey() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 40
        if db.put(partition: partition, compositeKeys: ["ad",1,123,"test",p1.PersonId],ttl: nil,p1) {
            if let retrieved: Person = db.get(partition: partition, compositeKeys:["ad",1,123,"test",p1.PersonId]) {
                print("retrieved item with id \(retrieved.PersonId)")
                if retrieved.PersonId == p1.PersonId {
                    return
                }
            } else {
                XCTFail("failed to retrieve object")
            }
        }
        
        XCTFail("failed to write one of the records")
        
    }
    
    func testPersistMultipleIterate() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        if db.put(partition: partition, ttl: nil,p1) {
            p2.Name = "Neil Bostrom"
            p2.Age = 38
            if db.put(partition: partition, ttl: nil,p2) {
                p3.Name = "George Smith"
                p3.Age = 28
                if db.put(partition: partition, ttl: nil,p3) {
                    var results: [Person] = []
                    db.iterate(partition: partition, keyspace: p1.keyspace) { (person: Person) in
                        results.append(person)
                    }
                    if results.count == 3 {
                        return
                    } else {
                        XCTFail("failed to read back the correct number of records")
                    }
                }
            }
        }
        XCTFail("failed to write one of the records")
    }
    
    func testPersistMultipleIterateInspect() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        if db.put(partition: partition, ttl: nil,p1) {
            p2.Name = "Neil Bostrom"
            p2.Age = 38
            if db.put(partition: partition, ttl: nil,p2) {
                p3.Name = "George Smith"
                p3.Age = 28
                if db.put(partition: partition, ttl: nil,p3) {
                    var results: [Person] = []
                    db.iterate(partition: partition, keyspace: p1.keyspace) { (person: Person) in
                        if person.Age == 38 {
                            results.append(person)
                        }
                    }
                    if results.count == 1 {
                        return
                    } else {
                        XCTFail("failed to read back the correct number of records")
                    }
                }
            }
        }
        XCTFail("failed to write one of the records")
    }
    
    func testTTLTimeout() {
        
        let db = initPostgresDatabase()
        let partition = "\(UUID().uuidString.lowercased().prefix(8))"
        
        let p1 = Person()
        let p2 = Person()
        let p3 = Person()
        
        p1.Name = "Adrian Herridge"
        p1.Age = 41
        
        p2.Name = "Neil Bostrom"
        p2.Age = 38
        
        p3.Name = "George Smith"
        p3.Age = 28
        
        let _ = db.put(partition: partition, ttl: 1, p1)
        let _ = db.put(partition: partition, ttl: 60, p2)
        let _ = db.put(partition: partition, ttl: nil, p3)
        
        Thread.sleep(forTimeInterval: 2.0)
        
        let results: [Person] = db.all(partition: partition, keyspace: p1.keyspace)
        
        if results.count == 2 {
            return
        } else {
            XCTFail("failed to read back the correct number of records")
        }
        
        XCTFail("failed to write one of the records")
    }
    
    func testObjectMigration() {
        
        let db = initPostgresDatabase()
        
        let id = UUID()
        
        let p1 = PersonVersion1()
        p1.id = id
        p1.name = "Adrian Herridge"
        p1.age = 40
        
        if db.put(key: id, p1) {
            if let _: PersonVersion1 = db.get(key: id) {
                db.migrate(from: PersonVersion1.self, to: PersonVersion2.self) { old in
                    
                    let new = PersonVersion2()
                    
                    new.id = old.id
                    new.age = old.age
                    
                    let components = old.name?.components(separatedBy: " ")
                    new.forename = components?.first
                    new.surname = components?.last
                    
                    return new
                }
                if let updated: PersonVersion2 = db.get(key: id) {
                    if updated.forename == "Adrian" && updated.surname == "Herridge" {
                        return
                    }
                }
            }
        }
        
        XCTFail("failed to write one of the records")
        
    }
    
    func testFilterMultiple() {
        
        let db = initPostgresDatabase()
        
        
        let p1 = Person()
        p1.Name = "Adrian Herridge"
        p1.Age = 40
        db.put(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "true", "extravar" : "123"], p1)
        
        let p2 = Person()
        p2.Name = "Adrian Herridge"
        p2.Age = 40
        db.put(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "true"], p2)
        
        let p3 = Person()
        p3.Name = "Adrian Herridge"
        p3.Age = 40
        db.put(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "false"], p3)
        
        let results: [Person] = db.all(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "true"])
        if results.count == 2 {
            return
        }
        
        XCTFail("failed to write one of the records")
        
    }
    
    func testFilterMultipleAND() {
        
        let db = initPostgresDatabase()
        
        
        let p1 = Person()
        p1.Name = "Adrian Herridge"
        p1.Age = 40
        db.put(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "true", "extravar" : "123"], p1)
        
        let p2 = Person()
        p2.Name = "Adrian Herridge"
        p2.Age = 40
        db.put(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "true"], p2)
        
        let p3 = Person()
        p3.Name = "Adrian Herridge"
        p3.Age = 40
        db.put(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "false"], p3)
        
        let results: [Person] = db.all(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "true", "extravar" : "123"])
        if results.count == 1 {
            return
        }
        
        XCTFail("failed to write one of the records")
        
    }
    
    func testFilterMultipleNegative() {
        
        let db = initPostgresDatabase()
        
        
        let p1 = Person()
        p1.Name = "Adrian Herridge"
        p1.Age = 40
        db.put(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "true", "extravar" : "1234"], p1)
        
        let p2 = Person()
        p2.Name = "Adrian Herridge"
        p2.Age = 40
        db.put(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "true"], p2)
        
        let p3 = Person()
        p3.Name = "Adrian Herridge"
        p3.Age = 40
        db.put(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "false"], p3)
        
        let results: [Person] = db.all(partition: "default", keyspace: p1.keyspace, filter: ["crazyvar" : "true", "extravar" : "123"])
        if results.count == 0 {
            return
        }
        
        XCTFail("failed to write one of the records")
        
    }
    
    func testFilterProtocolConformance() {
        
        let db = initPostgresDatabase()
        
        
        let p1 = PersonFilterable()
        p1.Name = "Adrian Herridge"
        p1.Age = 40
        db.put(p1)
        
        let p2 = PersonFilterable()
        p2.Name = "Neil Bostrom"
        p2.Age = 40
        db.put(p2)
        
        let p3 = PersonFilterable()
        p3.Name = "Sarah Herridge"
        p3.Age = 40
        db.put(p3)
        
        let results: [PersonFilterable] = db.all(keyspace: "person", filter: ["age" : "40"])
        if results.count != 3 {
            XCTFail("failed to get the correct filtered records")
        }
        
        let results2: [PersonFilterable] = db.all(keyspace: "person", filter: ["name" : "Neil Bostrom"])
        if results2.count != 1 {
            XCTFail("failed to get the correct filtered records")
        }
        
    }
    
}

