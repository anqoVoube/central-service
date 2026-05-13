//! One-shot migration: rewrite `compute_unit_limit` for the 112 pools that
//! were measured during the 2026-05-13 10:39–10:41 run, replacing the
//! +1%-padded values with the raw `cu_consumed` ones from the log. Future
//! measurements (after the `measure_cu.rs` change that landed alongside
//! this binary) will store raw directly.
//!
//! Bot applies the +1% safety margin at fire time — single source of truth.
//!
//! Run from a directory whose `.env` has `MONGO_URI` and `MONGO_DB`:
//!
//!     cd ~/Work/central-service-seed
//!     cargo run --release --bin migrate_cu_to_raw \
//!         --manifest-path ~/Work/central-service/Cargo.toml
//!
//! Or build once, run by path:
//!
//!     cd ~/Work/central-service && cargo build --release --bin migrate_cu_to_raw
//!     cd ~/Work/central-service-seed && ~/Work/central-service/target/release/migrate_cu_to_raw

use anyhow::Context;
use central_service::mongo::Repo;

/// (pool_pubkey, raw cu_consumed) pairs lifted verbatim from the 2026-05-13
/// measure_cu run log. 112 entries — the 3 pools that hit ATA-missing
/// during that run are excluded.
const PAIRS: &[(&str, i32)] = &[
    ("7YA9VxHmeGkrWt67RfbLA1hEWA4U84doiv2ej3gcQz5u", 115923),
    ("HkYoT24A8yukYXzGSjbubh9XhiDx6T36NWk14s2rHpsH", 112980),
    ("4EiA7PWpY2vPF7nPiAdmqLa8qkVNtiPHVeYYFBwHexmV", 108480),
    ("GajXCZLDrq85ZdE6iPLppssLQpZTdUSAu14tHLGudk7h", 117641),
    ("AbmubLE94CYV93aD5rSqsj5La27BaZRgHhZDDJkVSKdX", 115913),
    ("CoFqqYaX5k9UL45UmeMdMRXcGShLDqAATEEXzkTXnwwa", 111479),
    ("eKShgykXZxNrJ9yRQRP8RhcLRs7TeM2LpjPdPN8pE1e",  115980),
    ("BCrCkcnoQDRcPFBPfuAHP61BnxtNNmBCXnDfRko5d3Dr", 109980),
    ("4tJ9DY3nLZ8WQdGsNs3a8BZreXzCdQFAe7ZVT4vpqwBK", 111100),
    ("3eZZeSWonS4Phu1nVY6wsCQWftajKrgEeLiai63f9u9s", 136827),
    ("Ec9hBQJSv6UAaiu5roG4QX4roWHzmjhaELdc5ByWoP9e", 113153),
    ("BUZrkdmY94NDwo3N4QdAtLcsu4RE3p556CKnXe9dqYAv", 123412),
    ("68CDFH5L8WGaqATtLoeRTwAWoES6H9WRPWfCbKMmokPx", 120308),
    ("8npXE8Pp4zsQxgnvGmPaLjoWcfUDEosFGxrWC72TU6AB", 115827),
    ("FHgv8qmEwvgQ6AvosbeiUK6g2AsdRNda6GG3aiiNydU1", 114327),
    ("5cBZrQS6K5fyahmrUMFHWxQX2XkSjkTYscNEAMHF3RR8", 120326),
    ("9d2D6o6GQsmAA3Yta5XxryMceoM4RW5qcDMvs72FB4Gr", 114327),
    ("42GuVBqNbckL4cbwGC5nc6nDK9Be9uPfgtwiNvRvYG9t", 122078),
    ("73nMgXPbhuKs7yhk7Spw66bAoL7NBB9GMAPLwn2NHVJz", 114325),
    ("BLKv4xtt5zc9i85NpSyj4iHi5qh4UZdbjgWNT8BzY35Y", 111479),
    ("xLyaTohEaiqbPtySXejZC1M4Z4WSHHwPD39LvuFaoa4",  114326),
    ("3RNdNEHuJR9kmDF5H7smKXPFgAC4B1yASFZBXqyT9AAq", 111480),
    ("6YAbe7Y4SCVcVfUXSnXpFXN1y3PfNQignGKALra7UDRk", 109980),
    ("7NRF6JmKroKtr5qhoBD9jQ4taXyQRZPXh5s3yWuevTRS", 111579),
    ("DWmLHKSbbozFA47f2eSofRy9PGp326dMmMJF5WvQ1vHH", 111480),
    ("J6hybSEsywufv8frkQiwU3hKDU9fb3pwynCFSA14sxTu", 108464),
    ("C3ZvecR6rxGVFgarVi9u9kEokCozTxRp2L7TCTdx9AS8", 106796),
    ("6E5j2ezf4qXish7kdvvYQXaJaEZUhsQjfsaEdvKJu9TR", 123326),
    ("FmVzeH7MX89aPPTQF876w9qjfjH4yPQk5qZFmb9wcr9b", 118922),
    ("3NG2aQFbmoApXJms5iKdcWfEUcQpnQSup7GGJyxgoakJ", 115925),
    ("6ZgRwW3UcGdyRAiJ3NwGYHszeqs4WzAABFTJRceboFVP", 120157),
    ("A87mUrDQthRJF3grX6AM9WpBZgcLjTUosZMbsDHqvnwH", 115927),
    ("3fv6FDou8RMsu8su9iKS7S2oaJs2h7MqHYWq9b4kwqvf", 118913),
    ("6P4bsNMftKDJVYt1ukETtD28iKnxQvySB6382zC9AbD5", 113076),
    ("9VHypymHvu3XF8T9Lb66269bHfn7VaNSzK8KWXxaG9vU", 111655),
    ("GG2s2nr5D5g6BQZo2QWSGSLXtCgxyXwUPVr3EYLAXzar", 117478),
    ("4PL691pTDPXxu8gDtbn3gwjUfc3iZJ7pv2RTUPSq24Fp", 117327),
    ("tJ7RAt9UbiGUgPxsxBpcEeipGvacpzJVG6z1nAJRRcK",  108480),
    ("3iQuS3k7YZzYmgBnFF3Ye5K6T1JfspmPfXxmZf8pJetB", 109980),
    ("Fej6xTZ4w2EgTUtAsDe7Dzdtq3T1oRDfGeQbXeWAw37w", 114266),
    ("5nBm4wVTrnmpScMdVJ5anAwkAqMqG4NueBXMgRnwtarS", 114310),
    ("CdnaUHoapzLPKbLPh86ygVNB2Vo7uHLXiNBBgW8gUxvF", 123326),
    ("6CdyLyox6uUtvNP4ahHjsff9a8w7nZPWe5Zb7ddaPYZB", 108480),
    ("2wdWReqFB67ErE9YfDhCpL2dvMR5SnP7CkcDHrtM1osN", 112600),
    ("69vfYvsbZcgfbKiqQwSXmUHaqT2A1zuXYvWZAvzpQHVM", 114254),
    ("4p6uBW95FaAnBdXeDosTC5JAaszFqPAmgFTox4mShAQM", 109980),
    ("A6dYciEvGusaDfNebuUfv9kzL17ptm9edwhpZAv2MhA4", 117327),
    ("AzWEh5vHtqznVeqde7F2uCRSitBY42GQMTBAEWgQgthr", 114575),
    ("3Gg6dqjLt1twtarENWi9VSnrTtM6t3AKkM3Y46HyPWsX", 115796),
    ("m6TDTdNKiuZM3TURsRz6vh5YJAfpasEWMMfjPQh9KHa",  111309),
    ("41EMZ9LSLvFjhmDGvqP1DsA8uaEYq3fQp4bn3VWbqkNB", 109979),
    ("DCNxYC4yRa4phjyCUSa8WWYQXrKTpbvSiYcpKQSKQ4Ue", 112827),
    ("DCPt5Qjw7K1jZfti8SUWKhKwEJNLG3nvkY7Fmervz3FA", 114313),
    ("DnaQqhKPqgxm3WPyhUNZb69CT8oaZxAXf1MrXyquJFAm", 114155),
    ("8csZhu1YQBa5t91yck2QiZJUeR17yLLsgmo4YrXANLfu", 114640),
    ("EYjhX2C7xB1mCjgT9LQJr9ZTZbJYqwkg8Cf3eUGSzwoB", 109796),
    ("34UtPx3zyYfC5GVqDXWJqLf77BU5Ebb6o3C2XynpKtzL", 111254),
    ("8u8jyWDKVjPhbpGtzbQzyxzgFoqFum5aEikCiWp6zqQb", 110155),
    ("2VFKXw4TcTioJUfZQvZnirVXoHbtuwNRNUkXoVLJG7Me", 117312),
    ("EMGbjMzDTrNH32pxwvVHV2NrM8nZBGdAFnopo2yYGFmo", 111576),
    ("HywA1RBy2Yw6Sr6EcDmv1tQo7eu6uYuHnQEpWPwConkQ", 111478),
    ("AKaA2SBTsZQmgzq2r7zGiZYTsGqkDmGoQ71QoA8pSZnt", 115642),
    ("8uk8H6QSG4fsWvCAMcCn9TheoBiPYV667aCLVCwoBuMs", 109977),
    ("5MVZJTwnKLSuNL11quSrU5hq2vgzj7bBGZq7W3qvPrMa", 120489),
    ("6rbUbDZjNXz2bzvdKxACv6eDQQgzfU1VHbpX2djQFGyY", 120326),
    ("Aj1uN461vpAbBDaRM4X3XzTwsCAVEW3KaHS3obn3Rtd2", 115826),
    ("EA4922TkoBuBPwyJe2uiSHt3DiiaWFQf7akpsia9ZxWa", 120479),
    ("EtLebJkGC4CrKo1dgvnHtYn1KCkps9KTrwR6VhupTM33", 109795),
    ("2H1WiVoJyx1HnHayCGQqQ8UjcAWPCy7TYjNReg9Nq2uW", 117655),
    ("6gvXCTJKdMFS4eCbMatqQAyCjztRCCUAfqhRYvVhQysh", 111480),
    ("TiUk63PHr5xXhT36wttTTssJVExiMDSyoDKRpZf8iFP",  109984),
    ("79ELYKnQAwgU4p2Sf9v5Dc5hfix7mmQrYnD5buj6Rtv3", 112943),
    ("3JfUqNC9QGuhBgrYcpZdqGxy7PZtoGH2EZqk2e6B6xGa", 120327),
    ("APXZhSYvAunuuXAKTX6Bsskv4Astf2VWvV7T7E5mFxMT", 109979),
    ("8XBiCbL2BRgCDpV8mHwAxXKWwRuf7rQhUtyCFP6JC6t8", 108489),
    ("BpTFBmaFXAL6mDBxCC3FuL4hbozSFXtz6SAdbLF9LHWP", 120480),
    ("Arpq5MsQj3WcM8e2aZJG4FMbz8agu8WYXsb5GiyrGkj4", 114309),
    ("Fx1CQNPPQthYzcvbLcsrW5gnEnjEJFHQTpsoQMFZ9cxr", 106795),
    ("296oSG3PNh91t4A7hoxRs2YptpAhiScD8XEktqLmKc9M", 117313),
    ("9xQHawEGCFjjtoaQ9At6jQzvXm3ShaEyW59kMRsNSF5H", 114326),
    ("FzbYhPcoCuA2qSrV1s3j38RcQhP6c943p3KHYb6C5rCq", 112473),
    ("5PQGMdXBUUnsPt6XjxuAopGHboCwdoqqa2gEmZXuefmj", 108310),
    ("78wSYbXvREWu78D814FQBkymcrpTdMxvd7wavsLDwt4Y", 120326),
    ("CoejJJUxB4dvqti7RhX9RWPHtdwN4ff79R67L4srHpbG", 108575),
    ("GoK5qWTjBDyecJj3nhQ5ty8V7nWdQR77j6VvvwdEfvD6", 114479),
    ("44GJhJvGyz3oKzLF9LzN2XF7a8yrQM9ecikcjLVCUEZv", 126296),
    ("eFNjQm47KDyBSrfxjs257cHdpE2QAayXZRJNjxtJiCK",  108579),
    ("EA3GM93TXozutgVpB2ZTpyyH92tCfpJdCXRXRg5g2c5q", 117443),
    ("798TDA7Cf8RXgz9mkoJB4hmsZDE2PaxYwHUChbaiG59L", 118827),
    ("VCRvsrGNycLHKspffE2dNw1vpFPwA8xQPKbgJeiftVV",  114087),
    ("ETMhxtENfkMK85TAcveEbZdBv9htziWzDSddmShRP2wB", 111156),
    ("Atx6zyC1WkbBpCwhZG8UZ2n51184Ps78zTQMzsMV2uDy", 106768),
    ("6qpqHrjqvEXYgg59y487WjvTuRqxsyHm6po5dF5t9m5H", 115980),
    ("HTdqavF7bc7EVTkbnkgMsFVLng2Uo2xD9jm79YPhpQaF", 112827),
    ("CxQT4kt95kuVVpVqfpBXYh5uEWj4W8DcAE8mdWADTtyS", 117327),
    ("ApkeAbYf6J3o41N8r9TycoCdhMrWqUvvfJeii6td8WFx", 115827),
    ("5XrPcbMjqxvryLYdpQqmEe7zthYiqayPezjgNY2mXqDs", 115826),
    ("AN5jsGh2V8mTiSPvEz1w3oJMU8FTvGcgnzi7v9qZHmw7", 111465),
    ("DdLisrxaTmU34u3U573YyWm8W8dBJazGbAtZpt9uc7fL", 112979),
    ("HbAiv6CwktN3xqcH9qCknqfXKT2QZc51rwgZgnm9UJft", 121812),
    ("AadyXT41kzRoy82cWF1KKJFMV8T1BeLtMwUwacsMPkkU", 112809),
    ("Gh7H8QVhjqQGyUb5shJaAAPZLRnBMkD79s2uMFWh4Pfu", 117479),
    ("8bxEM9XNgBiLu6zs4iyagUVjewXQjA5kjyDLcLeEuCPc", 112810),
    ("AJYgDEQsZcrrdo7zzRBYikz1ASrjPjQDAVzn41QQxgwL", 114295),
    ("EnKn6232nXa7y6Pyrt5gZ4pvU9KPG72LocC7VLz9AEfG", 108479),
    ("83S4T3u6b2RrWNArv6LtAR82pWgDi54TduDBDKxrBQtP", 117156),
    ("6hdxYPTTdeJ3Z7dXDqdefRxwzDEp8DjaW1qFhqXUyQpw", 112980),
    ("3AJGUmeQZbcUetEWwigkKqe4RbzBem1smeG24iJemxgs", 111479),
    ("SkzFM2tua894dPSN3BweVy2TPfRaMLvc3ZpiYajppsZ",  118989),
    ("Cf51Hk5Zm7JtMaqzkR9kj6AD8o1PeuwMeFf4z947XQ55", 108240),
    ("4K7vNYk6RCJtbTzLWC2gyCWv5Jvk7ion3R99tuBaTrhK", 117479),
    ("E4w9FSBStG1E3zdFEgbtifevXeQrXcWZexaiXh5Uxb8H", 108268),
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Skip the full `Config::from_env()` so this binary runs from any
    // .env that only carries Mongo creds (e.g. central-service-seed).
    let mongo_uri = std::env::var("MONGO_URI").context("MONGO_URI not set")?;
    let mongo_db = std::env::var("MONGO_DB").context("MONGO_DB not set")?;
    tracing::info!(db = %mongo_db, "migrating {} pools to raw cu", PAIRS.len());

    let repo = Repo::connect(&mongo_uri, &mongo_db).await?;

    let mut ok = 0usize;
    let mut failed = 0usize;
    for (i, (pool, raw_cu)) in PAIRS.iter().enumerate() {
        match repo.update_cu_limit(pool, *raw_cu).await {
            Ok(()) => {
                tracing::info!("[{}/{}] {} → {} (raw)", i + 1, PAIRS.len(), pool, raw_cu);
                ok += 1;
            }
            Err(e) => {
                tracing::warn!("[{}/{}] {} update failed: {:#}", i + 1, PAIRS.len(), pool, e);
                failed += 1;
            }
        }
    }

    tracing::info!(ok, failed, "migration done");
    Ok(())
}
