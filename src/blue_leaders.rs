//! Blue-star validator set: the TOP 24 leaders on whose slots the
//! competitor wallet `MRiYA4oN3158fCV8evhuCofrDzbHyYvYnGZUDJvoCsa` landed the
//! most successful transactions, from ~18h of its on-chain history. These
//! 24 cover ~50% of all mriya landings (the long tail of 500+ one-off
//! leaders is deliberately excluded). The dashboard renders a BLUE star on a
//! copy-trade whose OUR-slot leader is in this set.
//!
//! Static on purpose: regenerated from the collection script when refreshed.
//! No lazy static / extra deps — a 24-entry linear `contains` over &str is
//! trivial and runs only at dashboard serve time.

use solana_sdk::pubkey::Pubkey;

/// Validator IDENTITY pubkeys (base58), ordered by mriya landed-tx count desc.
pub const BLUE_LEADERS: &[&str] = &[
    "Fd7btgySsrjuo25CJCj7oE7VPMyezDhnx7pZkj2v69Nk",  // 1433 landed
    "HEL1USMZKAL2odpNBj2oCjffnFGaYwmbGmyewGv1e2TU",  // 638 landed
    "5pPRHniefFjkiaArbGX3Y8NUysJmQ9tMZg3FrFGwHzSm",  // 248 landed
    "DRpbCBMxVnDK7maPM5tGv6MvB3v1sRMC86PZ8okm21hy",  // 230 landed
    "JUPiTERrZqgf1jUyR7dSkhMx4Kn2qJyekWsg3LT1h4b",  // 174 landed
    "CAo1dCGYrB6NhHh5xb1cGjUiu86iyCfMTENxgHumSve4",  // 119 landed
    "E1r4Psq84tHfQ6aPTvvDka4U3u8zPVD7gEUrH25RdxHL",  // 118 landed
    "9eGrDohdNTAo61DRHyfMuqKWXqYnA3i254Wiszxe8FoY",  // 104 landed
    "EvnRmnMrd69kFdbLMxWkTn1icZ7DCceRhvmb2SJXqDo4",  // 97 landed
    "C8Bey3LKVJHVqN6xPTeW8WJfUgFQAeGNBpT4Rp99JP1k",  // 87 landed
    "8tjFeSApQ85ThoQXT28acfF2KUfQr3TvTdirSkzNnYC7",  // 86 landed
    "GnC339vkyXRm1jRX69dt9mapPPu2LbzXfSDoxc91qta6",  // 82 landed
    "5EhGYUyQNrxgUbuYF4vbL2SZDT6RMfhq3yjeyevvULeC",  // 80 landed
    "JD549HsbJHeEKKUrKgg4Fj2iyv2RGjsV7NTZjZUrHybB",  // 80 landed
    "J6etcxDdYjPHrtyvDXrbCkx3q9W1UjMj1vy1jBFPJEbK",  // 77 landed
    "5Cchr1XGEg7dbBXByV5NY2ad8jfxAM7HA3x8D56rq9Ux",  // 75 landed
    "HpcB5Qg8Y9E73dUkot5e8HkgAJbExsYeUzniY4bCuKac",  // 68 landed
    "krakeNd6ednDPEXxHAmoBs1qKVM8kLg79PvWF2mhXV1",  // 68 landed
    "ChorusmmK7i1AxXeiTtQgQZhQNiXYU84ULeaYF1EH15n",  // 67 landed
    "9jxgosAfHgHzwnxsHw4RAZYaLVokMbnYtmiZBreynGFP",  // 65 landed
    "BtsmiEEvnSuUnKxqXj2PZRYpPJAc7C34mGz8gtJ1DAaH",  // 64 landed
    "6y7V8dL673XFzm9QyC5vvh3itWkp7wztahBd2yDqsyrK",  // 64 landed
    "5ejbTALcBsKQ7Cj1iSuu2mY5jqbYHqh9gF5ERXLiYj1z",  // 59 landed
    "9UM8wQ8F5oMiRcP5YdqD6Lr4krpBWCD8LtgQYoisJd9i",  // 57 landed
];

/// True when this identity is in the blue-star set.
pub fn is_blue(identity: &Pubkey) -> bool {
    let s = identity.to_string();
    BLUE_LEADERS.contains(&s.as_str())
}
