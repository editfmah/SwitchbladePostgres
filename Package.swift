// swift-tools-version:5.5
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SwitchbladePostgres",
    platforms: [
        .macOS(.v10_15)
    ],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "SwitchbladePostgres",
            targets: ["SwitchbladePostgres"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package(url: "https://github.com/editfmah/switchblade.git", .branch("master")),
        .package(url: "https://github.com/vapor/postgres-kit.git", .exact("2.3.3")),
        .package(url: "https://github.com/krzyzanowskim/CryptoSwift.git",   .upToNextMinor(from: "1.3.8")),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "SwitchbladePostgres",
            dependencies: [.product(name: "Switchblade", package: "switchblade"), .product(name: "PostgresKit", package: "postgres-kit", condition: .when(platforms: [.linux,.macOS])),"CryptoSwift"]),
        .testTarget(
            name: "SwitchbladePostgresTests",
            dependencies: ["SwitchbladePostgres"]),
    ]
)
