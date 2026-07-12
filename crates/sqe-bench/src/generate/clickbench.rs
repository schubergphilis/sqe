use std::sync::Arc;

use arrow_array::{Date32Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{parquet_writer, BenchmarkGenerator, GenerateStats, TableDef};

pub struct ClickBenchGenerator;

// ---------------------------------------------------------------------------
// Row scale factor
// ---------------------------------------------------------------------------
//
// Small mode (default): SF × 100_000 rows of synthetic data.
// At SF=1.0 that is 100,000 rows — enough for correctness testing without
// downloading the 14 GB real dataset.
//
// To use the real ~100M-row Yandex dataset, download it separately:
//   wget https://datasets.clickhouse.com/hits_compatible/hits.parquet
// and place it at <output_dir>/clickbench/sf<scale>/hits/00000.parquet.

const ROWS_PER_SF: f64 = 100_000.0;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

fn hits_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("WatchID", DataType::Int64, false),
        Field::new("JavaEnable", DataType::Int32, false),
        Field::new("Title", DataType::Utf8, true),
        Field::new("GoodEvent", DataType::Int32, false),
        Field::new("EventTime", DataType::Int64, false),
        Field::new("EventDate", DataType::Date32, false),
        Field::new("CounterID", DataType::Int32, false),
        Field::new("ClientIP", DataType::Int32, false),
        Field::new("RegionID", DataType::Int32, false),
        Field::new("UserID", DataType::Int64, false),
        Field::new("CounterClass", DataType::Int32, false),
        Field::new("OS", DataType::Int32, false),
        Field::new("UserAgent", DataType::Int32, false),
        Field::new("URL", DataType::Utf8, true),
        Field::new("Referer", DataType::Utf8, true),
        Field::new("IsRefresh", DataType::Int32, false),
        Field::new("RefererCategoryID", DataType::Int32, false),
        Field::new("RefererRegionID", DataType::Int32, false),
        Field::new("URLCategoryID", DataType::Int32, false),
        Field::new("URLRegionID", DataType::Int32, false),
        Field::new("ResolutionWidth", DataType::Int32, false),
        Field::new("ResolutionHeight", DataType::Int32, false),
        Field::new("ResolutionDepth", DataType::Int32, false),
        Field::new("FlashMajor", DataType::Int32, false),
        Field::new("FlashMinor", DataType::Int32, false),
        Field::new("FlashMinor2", DataType::Utf8, true),
        Field::new("NetMajor", DataType::Int32, false),
        Field::new("NetMinor", DataType::Int32, false),
        Field::new("UserAgentMajor", DataType::Int32, false),
        Field::new("UserAgentMinor", DataType::Utf8, true),
        Field::new("CookieEnable", DataType::Int32, false),
        Field::new("JavascriptEnable", DataType::Int32, false),
        Field::new("IsMobile", DataType::Int32, false),
        Field::new("MobilePhone", DataType::Int32, false),
        Field::new("MobilePhoneModel", DataType::Utf8, true),
        Field::new("Params", DataType::Utf8, true),
        Field::new("IPNetworkID", DataType::Int32, false),
        Field::new("TraficSourceID", DataType::Int32, false),
        Field::new("SearchEngineID", DataType::Int32, false),
        Field::new("SearchPhrase", DataType::Utf8, true),
        Field::new("AdvEngineID", DataType::Int32, false),
        Field::new("IsArtifical", DataType::Int32, false),
        Field::new("WindowClientWidth", DataType::Int32, false),
        Field::new("WindowClientHeight", DataType::Int32, false),
        Field::new("ClientTimeZone", DataType::Int32, false),
        Field::new("ClientEventTime", DataType::Int64, false),
        Field::new("SilverlightVersion1", DataType::Int32, false),
        Field::new("SilverlightVersion2", DataType::Int32, false),
        Field::new("SilverlightVersion3", DataType::Int32, false),
        Field::new("SilverlightVersion4", DataType::Int32, false),
        Field::new("PageCharset", DataType::Utf8, true),
        Field::new("CodeVersion", DataType::Int32, false),
        Field::new("IsLink", DataType::Int32, false),
        Field::new("IsDownload", DataType::Int32, false),
        Field::new("IsNotBounce", DataType::Int32, false),
        Field::new("FUniqID", DataType::Int64, false),
        Field::new("OriginalURL", DataType::Utf8, true),
        Field::new("HID", DataType::Int32, false),
        Field::new("IsOldCounter", DataType::Int32, false),
        Field::new("IsEvent", DataType::Int32, false),
        Field::new("IsParameter", DataType::Int32, false),
        Field::new("DontCountHits", DataType::Int32, false),
        Field::new("WithHash", DataType::Int32, false),
        Field::new("HitColor", DataType::Utf8, true),
        Field::new("LocalEventTime", DataType::Int64, false),
        Field::new("Age", DataType::Int32, false),
        Field::new("Sex", DataType::Int32, false),
        Field::new("Income", DataType::Int32, false),
        Field::new("Interests", DataType::Int32, false),
        Field::new("Robotness", DataType::Int32, false),
        Field::new("RemoteIP", DataType::Int32, false),
        Field::new("WindowName", DataType::Int32, false),
        Field::new("OpenerName", DataType::Int32, false),
        Field::new("HistoryLength", DataType::Int32, false),
        Field::new("BrowserLanguage", DataType::Utf8, true),
        Field::new("BrowserCountry", DataType::Utf8, true),
        Field::new("SocialNetwork", DataType::Utf8, true),
        Field::new("SocialAction", DataType::Utf8, true),
        Field::new("HTTPError", DataType::Int32, false),
        Field::new("SendTiming", DataType::Int32, false),
        Field::new("DNSTiming", DataType::Int32, false),
        Field::new("ConnectTiming", DataType::Int32, false),
        Field::new("ResponseStartTiming", DataType::Int32, false),
        Field::new("ResponseEndTiming", DataType::Int32, false),
        Field::new("FetchTiming", DataType::Int32, false),
        Field::new("SocialSourceNetworkID", DataType::Int32, false),
        Field::new("SocialSourcePage", DataType::Utf8, true),
        Field::new("ParamPrice", DataType::Int64, false),
        Field::new("ParamOrderID", DataType::Utf8, true),
        Field::new("ParamCurrency", DataType::Utf8, true),
        Field::new("ParamCurrencyID", DataType::Int32, false),
        Field::new("OpenstatServiceName", DataType::Utf8, true),
        Field::new("OpenstatCampaignID", DataType::Utf8, true),
        Field::new("OpenstatAdID", DataType::Utf8, true),
        Field::new("OpenstatSourceID", DataType::Utf8, true),
        Field::new("UTMSource", DataType::Utf8, true),
        Field::new("UTMMedium", DataType::Utf8, true),
        Field::new("UTMCampaign", DataType::Utf8, true),
        Field::new("UTMContent", DataType::Utf8, true),
        Field::new("UTMTerm", DataType::Utf8, true),
        Field::new("FromTag", DataType::Utf8, true),
        Field::new("HasGCLID", DataType::Int32, false),
        Field::new("RefererHash", DataType::Int64, false),
        Field::new("URLHash", DataType::Int64, false),
        Field::new("CLID", DataType::Int32, false),
    ]))
}

// ---------------------------------------------------------------------------
// Reference data for realistic-looking synthetic strings
// ---------------------------------------------------------------------------

const SEARCH_PHRASES: &[&str] = &[
    "",
    "yandex",
    "maps yandex",
    "google search",
    "weather forecast",
    "online shopping",
    "news today",
    "football results",
    "youtube video",
    "recipe ideas",
    "bank transfer",
    "email login",
    "job vacancies",
    "apartment rental",
    "train schedule",
];

const MOBILE_MODELS: &[&str] = &[
    "",
    "iPhone",
    "Samsung Galaxy",
    "Huawei P30",
    "Xiaomi Mi",
    "Nokia 5",
    "LG G7",
    "Sony Xperia",
    "Motorola Edge",
    "OnePlus 9",
];

const BROWSER_LANGUAGES: &[&str] = &[
    "ru", "en-US", "en-GB", "de", "fr", "es", "it", "zh-CN", "ja", "pt-BR",
];

const BROWSER_COUNTRIES: &[&str] = &["RU", "US", "DE", "FR", "GB", "CN", "BR", "IN", "UA", "BY"];

const SOCIAL_NETWORKS: &[&str] = &[
    "",
    "vk.com",
    "facebook.com",
    "twitter.com",
    "instagram.com",
    "odnoklassniki.ru",
];

const SOCIAL_ACTIONS: &[&str] = &["", "like", "share", "comment", "follow", "repost"];

const HIT_COLORS: &[&str] = &["W", "G", "Y", "R", "B", "O"];

const PAGE_CHARSETS: &[&str] = &["UTF-8", "windows-1251", "ISO-8859-1", "UTF-16", "KOI8-R"];

const URL_PREFIXES: &[&str] = &[
    "http://example.com/page",
    "http://news.yandex.ru/article",
    "https://www.google.com/search?q=",
    "http://shop.ru/item",
    "https://social.ru/profile",
    "http://blog.example.org/post",
];

// ---------------------------------------------------------------------------
// Seeded literals
// ---------------------------------------------------------------------------
//
// The ClickBench query set probes values from the real Yandex dataset that a
// purely random generator never produces, which left q19/q22/q27/q28/q39/q42
// vacuous (0 rows on both engines in differential testing). Each pattern is
// seeded into ~0.5-1% of rows; everything else keeps its original
// distribution and the row count is unchanged.

/// q19 point lookup: WHERE "UserID" = 435090932899640449.
const SEEDED_USER_ID: i64 = 435_090_932_899_640_449;
const SEEDED_USER_ID_STRIDE: usize = 199;

/// q22 probes "Title" LIKE '%Google%' (with "URL" NOT LIKE '%.google.%' and
/// a non-empty "SearchPhrase").
const GOOGLE_TITLE_STRIDE: usize = 211;

/// q27 needs a CounterID with COUNT(*) > 100000 (reachable at full
/// 100M-row dataset scale with a ~1% hot counter) and q42 needs a CounterID
/// with more than 10 distinct UserIDs. 62 is the busiest counter in the
/// real dataset.
const HOT_COUNTER_ID: i32 = 62;
const HOT_COUNTER_STRIDE: usize = 100;

/// q28 groups referers by host with HAVING COUNT(*) > 100000; concentrate
/// ~1% of referers on a single dedicated host so one hot host group exists.
const HOT_REFERER_PREFIX: &str = "http://hot.example.ru/link";
const HOT_REFERER_STRIDE: usize = 101;

/// q39 probes "UTMSource" <> ''; ~1% of rows carry a UTM tag.
const UTM_SOURCES: &[&str] = &["google_ads", "yandex_direct", "newsletter", "partner_blog"];
const UTM_SOURCE_STRIDE: usize = 103;

// ---------------------------------------------------------------------------
// Seed derivation (consistent with other generators)
// ---------------------------------------------------------------------------

fn seed_for_table(name: &str) -> u64 {
    name.bytes()
        .enumerate()
        .fold(0u64, |acc, (i, b)| {
            acc ^ ((b as u64).wrapping_shl(i as u32 % 64))
        })
        .wrapping_add(0xC11C_1BE0_0000_0000)
}

// ---------------------------------------------------------------------------
// Synthetic hits generator
// ---------------------------------------------------------------------------

const BATCH_SIZE: usize = 10_000;

// EventDate baseline: 2013-07-01 (days since epoch)
// 2013-07-01 = 15887 days after 1970-01-01
const EVENT_DATE_BASE: i32 = 15_887;
const EVENT_DATE_RANGE: i32 = 31; // one month window

// EventTime baseline: 2013-07-01 00:00:00 UTC as unix timestamp
const EVENT_TIME_BASE: i64 = 1_372_636_800;
const EVENT_TIME_RANGE: i64 = 86_400 * 31;

fn random_url(rng: &mut StdRng) -> String {
    let prefix = URL_PREFIXES[rng.gen_range(0..URL_PREFIXES.len())];
    let id: u32 = rng.gen_range(1..100_000);
    format!("{prefix}/{id}")
}

fn random_opt_string<'a>(rng: &mut StdRng, pool: &[&'a str]) -> &'a str {
    pool[rng.gen_range(0..pool.len())]
}

fn generate_hits(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = hits_schema();
    let total = (scale * ROWS_PER_SF) as usize;
    let mut rng = StdRng::seed_from_u64(seed_for_table("hits"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);

        // Int64 columns
        let mut watch_id: Vec<i64> = Vec::with_capacity(n);
        let mut event_time: Vec<i64> = Vec::with_capacity(n);
        let mut user_id: Vec<i64> = Vec::with_capacity(n);
        let mut client_event_time: Vec<i64> = Vec::with_capacity(n);
        let mut f_uniq_id: Vec<i64> = Vec::with_capacity(n);
        let mut local_event_time: Vec<i64> = Vec::with_capacity(n);
        let mut param_price: Vec<i64> = Vec::with_capacity(n);
        let mut referer_hash: Vec<i64> = Vec::with_capacity(n);
        let mut url_hash: Vec<i64> = Vec::with_capacity(n);

        // Date32 columns
        let mut event_date: Vec<i32> = Vec::with_capacity(n);

        // Int32 columns
        let mut java_enable: Vec<i32> = Vec::with_capacity(n);
        let mut good_event: Vec<i32> = Vec::with_capacity(n);
        let mut counter_id: Vec<i32> = Vec::with_capacity(n);
        let mut client_ip: Vec<i32> = Vec::with_capacity(n);
        let mut region_id: Vec<i32> = Vec::with_capacity(n);
        let mut counter_class: Vec<i32> = Vec::with_capacity(n);
        let mut os: Vec<i32> = Vec::with_capacity(n);
        let mut user_agent: Vec<i32> = Vec::with_capacity(n);
        let mut is_refresh: Vec<i32> = Vec::with_capacity(n);
        let mut referer_category_id: Vec<i32> = Vec::with_capacity(n);
        let mut referer_region_id: Vec<i32> = Vec::with_capacity(n);
        let mut url_category_id: Vec<i32> = Vec::with_capacity(n);
        let mut url_region_id: Vec<i32> = Vec::with_capacity(n);
        let mut resolution_width: Vec<i32> = Vec::with_capacity(n);
        let mut resolution_height: Vec<i32> = Vec::with_capacity(n);
        let mut resolution_depth: Vec<i32> = Vec::with_capacity(n);
        let mut flash_major: Vec<i32> = Vec::with_capacity(n);
        let mut flash_minor: Vec<i32> = Vec::with_capacity(n);
        let mut net_major: Vec<i32> = Vec::with_capacity(n);
        let mut net_minor: Vec<i32> = Vec::with_capacity(n);
        let mut user_agent_major: Vec<i32> = Vec::with_capacity(n);
        let mut cookie_enable: Vec<i32> = Vec::with_capacity(n);
        let mut javascript_enable: Vec<i32> = Vec::with_capacity(n);
        let mut is_mobile: Vec<i32> = Vec::with_capacity(n);
        let mut mobile_phone: Vec<i32> = Vec::with_capacity(n);
        let mut ip_network_id: Vec<i32> = Vec::with_capacity(n);
        let mut trafic_source_id: Vec<i32> = Vec::with_capacity(n);
        let mut search_engine_id: Vec<i32> = Vec::with_capacity(n);
        let mut adv_engine_id: Vec<i32> = Vec::with_capacity(n);
        let mut is_artifical: Vec<i32> = Vec::with_capacity(n);
        let mut window_client_width: Vec<i32> = Vec::with_capacity(n);
        let mut window_client_height: Vec<i32> = Vec::with_capacity(n);
        let mut client_time_zone: Vec<i32> = Vec::with_capacity(n);
        let mut silverlight_version1: Vec<i32> = Vec::with_capacity(n);
        let mut silverlight_version2: Vec<i32> = Vec::with_capacity(n);
        let mut silverlight_version3: Vec<i32> = Vec::with_capacity(n);
        let mut silverlight_version4: Vec<i32> = Vec::with_capacity(n);
        let mut code_version: Vec<i32> = Vec::with_capacity(n);
        let mut is_link: Vec<i32> = Vec::with_capacity(n);
        let mut is_download: Vec<i32> = Vec::with_capacity(n);
        let mut is_not_bounce: Vec<i32> = Vec::with_capacity(n);
        let mut hid: Vec<i32> = Vec::with_capacity(n);
        let mut is_old_counter: Vec<i32> = Vec::with_capacity(n);
        let mut is_event: Vec<i32> = Vec::with_capacity(n);
        let mut is_parameter: Vec<i32> = Vec::with_capacity(n);
        let mut dont_count_hits: Vec<i32> = Vec::with_capacity(n);
        let mut with_hash: Vec<i32> = Vec::with_capacity(n);
        let mut age: Vec<i32> = Vec::with_capacity(n);
        let mut sex: Vec<i32> = Vec::with_capacity(n);
        let mut income: Vec<i32> = Vec::with_capacity(n);
        let mut interests: Vec<i32> = Vec::with_capacity(n);
        let mut robotness: Vec<i32> = Vec::with_capacity(n);
        let mut remote_ip: Vec<i32> = Vec::with_capacity(n);
        let mut window_name: Vec<i32> = Vec::with_capacity(n);
        let mut opener_name: Vec<i32> = Vec::with_capacity(n);
        let mut history_length: Vec<i32> = Vec::with_capacity(n);
        let mut http_error: Vec<i32> = Vec::with_capacity(n);
        let mut send_timing: Vec<i32> = Vec::with_capacity(n);
        let mut dns_timing: Vec<i32> = Vec::with_capacity(n);
        let mut connect_timing: Vec<i32> = Vec::with_capacity(n);
        let mut response_start_timing: Vec<i32> = Vec::with_capacity(n);
        let mut response_end_timing: Vec<i32> = Vec::with_capacity(n);
        let mut fetch_timing: Vec<i32> = Vec::with_capacity(n);
        let mut social_source_network_id: Vec<i32> = Vec::with_capacity(n);
        let mut param_currency_id: Vec<i32> = Vec::with_capacity(n);
        let mut has_gclid: Vec<i32> = Vec::with_capacity(n);
        let mut clid: Vec<i32> = Vec::with_capacity(n);

        // String columns (owned)
        let mut title: Vec<String> = Vec::with_capacity(n);
        let mut url: Vec<String> = Vec::with_capacity(n);
        let mut referer: Vec<String> = Vec::with_capacity(n);
        let mut flash_minor2: Vec<String> = Vec::with_capacity(n);
        let mut user_agent_minor: Vec<String> = Vec::with_capacity(n);
        let mut mobile_phone_model: Vec<String> = Vec::with_capacity(n);
        let mut params: Vec<String> = Vec::with_capacity(n);
        let mut search_phrase: Vec<String> = Vec::with_capacity(n);
        let mut page_charset: Vec<String> = Vec::with_capacity(n);
        let mut original_url: Vec<String> = Vec::with_capacity(n);
        let mut hit_color: Vec<String> = Vec::with_capacity(n);
        let mut browser_language: Vec<String> = Vec::with_capacity(n);
        let mut browser_country: Vec<String> = Vec::with_capacity(n);
        let mut social_network: Vec<String> = Vec::with_capacity(n);
        let mut social_action: Vec<String> = Vec::with_capacity(n);
        let mut social_source_page: Vec<String> = Vec::with_capacity(n);
        let mut param_order_id: Vec<String> = Vec::with_capacity(n);
        let mut param_currency: Vec<String> = Vec::with_capacity(n);
        let mut openstat_service_name: Vec<String> = Vec::with_capacity(n);
        let mut openstat_campaign_id: Vec<String> = Vec::with_capacity(n);
        let mut openstat_ad_id: Vec<String> = Vec::with_capacity(n);
        let mut openstat_source_id: Vec<String> = Vec::with_capacity(n);
        let mut utm_source: Vec<String> = Vec::with_capacity(n);
        let mut utm_medium: Vec<String> = Vec::with_capacity(n);
        let mut utm_campaign: Vec<String> = Vec::with_capacity(n);
        let mut utm_content: Vec<String> = Vec::with_capacity(n);
        let mut utm_term: Vec<String> = Vec::with_capacity(n);
        let mut from_tag: Vec<String> = Vec::with_capacity(n);

        for i in 0..n {
            let global = offset + i;
            let row_id = global as i64;
            let et = EVENT_TIME_BASE + rng.gen_range(0..EVENT_TIME_RANGE);
            let ed = EVENT_DATE_BASE + rng.gen_range(0..EVENT_DATE_RANGE);
            // q19: seed the probed UserID into ~0.5% of rows.
            let uid: i64 = if global % SEEDED_USER_ID_STRIDE == 7 {
                SEEDED_USER_ID
            } else {
                rng.gen()
            };
            // q22: seeded rows get a Google title, a URL without '.google.',
            // and a non-empty SearchPhrase so all three predicates can hold.
            let google_title = global % GOOGLE_TITLE_STRIDE == 11;
            let mut search = random_opt_string(&mut rng, SEARCH_PHRASES).to_string();
            if google_title && search.is_empty() {
                search = "google search".to_string();
            }
            let adv_eng: i32 = if search.is_empty() {
                0
            } else {
                rng.gen_range(1..20)
            };
            let is_mob: i32 = rng.gen_range(0..2);
            let mob_model = if is_mob == 1 {
                random_opt_string(&mut rng, MOBILE_MODELS).to_string()
            } else {
                String::new()
            };

            watch_id.push(row_id.wrapping_add(rng.gen::<i32>() as i64));
            java_enable.push(rng.gen_range(0..2));
            title.push(if google_title {
                format!("Google news digest {}", rng.gen_range(1..10_000u32))
            } else {
                format!("Page title {}", rng.gen_range(1..10_000u32))
            });
            good_event.push(1);
            event_time.push(et);
            event_date.push(ed);
            // q27/q42: a hot counter takes ~1% of rows.
            counter_id.push(if global % HOT_COUNTER_STRIDE == 0 {
                HOT_COUNTER_ID
            } else {
                rng.gen_range(1..100_000)
            });
            client_ip.push(rng.gen());
            region_id.push(rng.gen_range(1..10_000));
            user_id.push(uid);
            counter_class.push(rng.gen_range(0..10));
            os.push(rng.gen_range(0..100));
            user_agent.push(rng.gen_range(0..1000));
            url.push(if google_title {
                // q22 requires "URL" NOT LIKE '%.google.%' on these rows.
                format!("http://example.com/page/{}", rng.gen_range(1..100_000u32))
            } else {
                random_url(&mut rng)
            });
            // q28: ~1% of referers share one dedicated host.
            referer.push(if global % HOT_REFERER_STRIDE == 17 {
                format!("{HOT_REFERER_PREFIX}/{}", rng.gen_range(1..100_000u32))
            } else {
                random_url(&mut rng)
            });
            is_refresh.push(rng.gen_range(0..2));
            referer_category_id.push(rng.gen_range(0..1000));
            referer_region_id.push(rng.gen_range(0..10_000));
            url_category_id.push(rng.gen_range(0..1000));
            url_region_id.push(rng.gen_range(0..10_000));
            resolution_width.push(rng.gen_range(640..2560));
            resolution_height.push(rng.gen_range(480..1440));
            resolution_depth.push(rng.gen_range(16..32));
            flash_major.push(rng.gen_range(0..32));
            flash_minor.push(rng.gen_range(0..10));
            flash_minor2.push(rng.gen_range(0..10u32).to_string());
            net_major.push(rng.gen_range(0..10));
            net_minor.push(rng.gen_range(0..10));
            user_agent_major.push(rng.gen_range(0..100));
            user_agent_minor.push(rng.gen_range(0..100u32).to_string());
            cookie_enable.push(rng.gen_range(0..2));
            javascript_enable.push(rng.gen_range(0..2));
            is_mobile.push(is_mob);
            mobile_phone.push(if is_mob == 1 {
                rng.gen_range(1..200)
            } else {
                0
            });
            mobile_phone_model.push(mob_model);
            params.push(String::new());
            ip_network_id.push(rng.gen_range(0..1000));
            trafic_source_id.push(rng.gen_range(-1..10));
            search_engine_id.push(rng.gen_range(0..10));
            search_phrase.push(search);
            adv_engine_id.push(adv_eng);
            is_artifical.push(0);
            window_client_width.push(rng.gen_range(640..2560));
            window_client_height.push(rng.gen_range(480..1440));
            client_time_zone.push(rng.gen_range(-720..840));
            client_event_time.push(et);
            silverlight_version1.push(rng.gen_range(0..10));
            silverlight_version2.push(rng.gen_range(0..10));
            silverlight_version3.push(rng.gen_range(0..100_000));
            silverlight_version4.push(rng.gen_range(0..100));
            page_charset.push(random_opt_string(&mut rng, PAGE_CHARSETS).to_string());
            code_version.push(rng.gen_range(0..100_000_000));
            is_link.push(rng.gen_range(0..2));
            is_download.push(rng.gen_range(0..2));
            is_not_bounce.push(rng.gen_range(0..2));
            f_uniq_id.push(rng.gen());
            original_url.push(random_url(&mut rng));
            hid.push(rng.gen_range(0..1_000_000));
            is_old_counter.push(0);
            is_event.push(rng.gen_range(0..2));
            is_parameter.push(rng.gen_range(0..2));
            dont_count_hits.push(0);
            with_hash.push(rng.gen_range(0..2));
            hit_color.push(random_opt_string(&mut rng, HIT_COLORS).to_string());
            local_event_time.push(et);
            age.push(rng.gen_range(0..100));
            sex.push(rng.gen_range(0..2));
            income.push(rng.gen_range(0..8));
            interests.push(rng.gen_range(0..1000));
            robotness.push(rng.gen_range(0..10));
            remote_ip.push(rng.gen());
            window_name.push(rng.gen_range(0..100));
            opener_name.push(rng.gen_range(0..100));
            history_length.push(rng.gen_range(1..100));
            browser_language.push(random_opt_string(&mut rng, BROWSER_LANGUAGES).to_string());
            browser_country.push(random_opt_string(&mut rng, BROWSER_COUNTRIES).to_string());
            social_network.push(random_opt_string(&mut rng, SOCIAL_NETWORKS).to_string());
            social_action.push(random_opt_string(&mut rng, SOCIAL_ACTIONS).to_string());
            http_error.push(0);
            send_timing.push(rng.gen_range(0..5000));
            dns_timing.push(rng.gen_range(0..1000));
            connect_timing.push(rng.gen_range(0..1000));
            response_start_timing.push(rng.gen_range(0..2000));
            response_end_timing.push(rng.gen_range(0..5000));
            fetch_timing.push(rng.gen_range(0..10_000));
            social_source_network_id.push(rng.gen_range(0..10));
            social_source_page.push(String::new());
            param_price.push(rng.gen_range(-1..100_000));
            param_order_id.push(String::new());
            param_currency.push(String::new());
            param_currency_id.push(0);
            openstat_service_name.push(String::new());
            openstat_campaign_id.push(String::new());
            openstat_ad_id.push(String::new());
            openstat_source_id.push(String::new());
            // q39: "UTMSource" <> '' must select rows; ~1% carry a UTM tag.
            utm_source.push(if global % UTM_SOURCE_STRIDE == 13 {
                random_opt_string(&mut rng, UTM_SOURCES).to_string()
            } else {
                String::new()
            });
            utm_medium.push(String::new());
            utm_campaign.push(String::new());
            utm_content.push(String::new());
            utm_term.push(String::new());
            from_tag.push(String::new());
            has_gclid.push(0);
            referer_hash.push(rng.gen());
            url_hash.push(rng.gen());
            clid.push(rng.gen_range(0..1000));
        }

        // Build string slices for Arrow
        let title_refs: Vec<&str> = title.iter().map(|s| s.as_str()).collect();
        let url_refs: Vec<&str> = url.iter().map(|s| s.as_str()).collect();
        let referer_refs: Vec<&str> = referer.iter().map(|s| s.as_str()).collect();
        let flash_minor2_refs: Vec<&str> = flash_minor2.iter().map(|s| s.as_str()).collect();
        let ua_minor_refs: Vec<&str> = user_agent_minor.iter().map(|s| s.as_str()).collect();
        let mob_model_refs: Vec<&str> = mobile_phone_model.iter().map(|s| s.as_str()).collect();
        let params_refs: Vec<&str> = params.iter().map(|s| s.as_str()).collect();
        let search_phrase_refs: Vec<&str> = search_phrase.iter().map(|s| s.as_str()).collect();
        let page_charset_refs: Vec<&str> = page_charset.iter().map(|s| s.as_str()).collect();
        let original_url_refs: Vec<&str> = original_url.iter().map(|s| s.as_str()).collect();
        let hit_color_refs: Vec<&str> = hit_color.iter().map(|s| s.as_str()).collect();
        let browser_language_refs: Vec<&str> =
            browser_language.iter().map(|s| s.as_str()).collect();
        let browser_country_refs: Vec<&str> = browser_country.iter().map(|s| s.as_str()).collect();
        let social_network_refs: Vec<&str> = social_network.iter().map(|s| s.as_str()).collect();
        let social_action_refs: Vec<&str> = social_action.iter().map(|s| s.as_str()).collect();
        let social_source_page_refs: Vec<&str> =
            social_source_page.iter().map(|s| s.as_str()).collect();
        let param_order_id_refs: Vec<&str> = param_order_id.iter().map(|s| s.as_str()).collect();
        let param_currency_refs: Vec<&str> = param_currency.iter().map(|s| s.as_str()).collect();
        let openstat_service_refs: Vec<&str> =
            openstat_service_name.iter().map(|s| s.as_str()).collect();
        let openstat_campaign_refs: Vec<&str> =
            openstat_campaign_id.iter().map(|s| s.as_str()).collect();
        let openstat_ad_refs: Vec<&str> = openstat_ad_id.iter().map(|s| s.as_str()).collect();
        let openstat_source_refs: Vec<&str> =
            openstat_source_id.iter().map(|s| s.as_str()).collect();
        let utm_source_refs: Vec<&str> = utm_source.iter().map(|s| s.as_str()).collect();
        let utm_medium_refs: Vec<&str> = utm_medium.iter().map(|s| s.as_str()).collect();
        let utm_campaign_refs: Vec<&str> = utm_campaign.iter().map(|s| s.as_str()).collect();
        let utm_content_refs: Vec<&str> = utm_content.iter().map(|s| s.as_str()).collect();
        let utm_term_refs: Vec<&str> = utm_term.iter().map(|s| s.as_str()).collect();
        let from_tag_refs: Vec<&str> = from_tag.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(watch_id)),
                    Arc::new(Int32Array::from(java_enable)),
                    Arc::new(StringArray::from(title_refs)),
                    Arc::new(Int32Array::from(good_event)),
                    Arc::new(Int64Array::from(event_time)),
                    Arc::new(Date32Array::from(event_date)),
                    Arc::new(Int32Array::from(counter_id)),
                    Arc::new(Int32Array::from(client_ip)),
                    Arc::new(Int32Array::from(region_id)),
                    Arc::new(Int64Array::from(user_id)),
                    Arc::new(Int32Array::from(counter_class)),
                    Arc::new(Int32Array::from(os)),
                    Arc::new(Int32Array::from(user_agent)),
                    Arc::new(StringArray::from(url_refs)),
                    Arc::new(StringArray::from(referer_refs)),
                    Arc::new(Int32Array::from(is_refresh)),
                    Arc::new(Int32Array::from(referer_category_id)),
                    Arc::new(Int32Array::from(referer_region_id)),
                    Arc::new(Int32Array::from(url_category_id)),
                    Arc::new(Int32Array::from(url_region_id)),
                    Arc::new(Int32Array::from(resolution_width)),
                    Arc::new(Int32Array::from(resolution_height)),
                    Arc::new(Int32Array::from(resolution_depth)),
                    Arc::new(Int32Array::from(flash_major)),
                    Arc::new(Int32Array::from(flash_minor)),
                    Arc::new(StringArray::from(flash_minor2_refs)),
                    Arc::new(Int32Array::from(net_major)),
                    Arc::new(Int32Array::from(net_minor)),
                    Arc::new(Int32Array::from(user_agent_major)),
                    Arc::new(StringArray::from(ua_minor_refs)),
                    Arc::new(Int32Array::from(cookie_enable)),
                    Arc::new(Int32Array::from(javascript_enable)),
                    Arc::new(Int32Array::from(is_mobile)),
                    Arc::new(Int32Array::from(mobile_phone)),
                    Arc::new(StringArray::from(mob_model_refs)),
                    Arc::new(StringArray::from(params_refs)),
                    Arc::new(Int32Array::from(ip_network_id)),
                    Arc::new(Int32Array::from(trafic_source_id)),
                    Arc::new(Int32Array::from(search_engine_id)),
                    Arc::new(StringArray::from(search_phrase_refs)),
                    Arc::new(Int32Array::from(adv_engine_id)),
                    Arc::new(Int32Array::from(is_artifical)),
                    Arc::new(Int32Array::from(window_client_width)),
                    Arc::new(Int32Array::from(window_client_height)),
                    Arc::new(Int32Array::from(client_time_zone)),
                    Arc::new(Int64Array::from(client_event_time)),
                    Arc::new(Int32Array::from(silverlight_version1)),
                    Arc::new(Int32Array::from(silverlight_version2)),
                    Arc::new(Int32Array::from(silverlight_version3)),
                    Arc::new(Int32Array::from(silverlight_version4)),
                    Arc::new(StringArray::from(page_charset_refs)),
                    Arc::new(Int32Array::from(code_version)),
                    Arc::new(Int32Array::from(is_link)),
                    Arc::new(Int32Array::from(is_download)),
                    Arc::new(Int32Array::from(is_not_bounce)),
                    Arc::new(Int64Array::from(f_uniq_id)),
                    Arc::new(StringArray::from(original_url_refs)),
                    Arc::new(Int32Array::from(hid)),
                    Arc::new(Int32Array::from(is_old_counter)),
                    Arc::new(Int32Array::from(is_event)),
                    Arc::new(Int32Array::from(is_parameter)),
                    Arc::new(Int32Array::from(dont_count_hits)),
                    Arc::new(Int32Array::from(with_hash)),
                    Arc::new(StringArray::from(hit_color_refs)),
                    Arc::new(Int64Array::from(local_event_time)),
                    Arc::new(Int32Array::from(age)),
                    Arc::new(Int32Array::from(sex)),
                    Arc::new(Int32Array::from(income)),
                    Arc::new(Int32Array::from(interests)),
                    Arc::new(Int32Array::from(robotness)),
                    Arc::new(Int32Array::from(remote_ip)),
                    Arc::new(Int32Array::from(window_name)),
                    Arc::new(Int32Array::from(opener_name)),
                    Arc::new(Int32Array::from(history_length)),
                    Arc::new(StringArray::from(browser_language_refs)),
                    Arc::new(StringArray::from(browser_country_refs)),
                    Arc::new(StringArray::from(social_network_refs)),
                    Arc::new(StringArray::from(social_action_refs)),
                    Arc::new(Int32Array::from(http_error)),
                    Arc::new(Int32Array::from(send_timing)),
                    Arc::new(Int32Array::from(dns_timing)),
                    Arc::new(Int32Array::from(connect_timing)),
                    Arc::new(Int32Array::from(response_start_timing)),
                    Arc::new(Int32Array::from(response_end_timing)),
                    Arc::new(Int32Array::from(fetch_timing)),
                    Arc::new(Int32Array::from(social_source_network_id)),
                    Arc::new(StringArray::from(social_source_page_refs)),
                    Arc::new(Int64Array::from(param_price)),
                    Arc::new(StringArray::from(param_order_id_refs)),
                    Arc::new(StringArray::from(param_currency_refs)),
                    Arc::new(Int32Array::from(param_currency_id)),
                    Arc::new(StringArray::from(openstat_service_refs)),
                    Arc::new(StringArray::from(openstat_campaign_refs)),
                    Arc::new(StringArray::from(openstat_ad_refs)),
                    Arc::new(StringArray::from(openstat_source_refs)),
                    Arc::new(StringArray::from(utm_source_refs)),
                    Arc::new(StringArray::from(utm_medium_refs)),
                    Arc::new(StringArray::from(utm_campaign_refs)),
                    Arc::new(StringArray::from(utm_content_refs)),
                    Arc::new(StringArray::from(utm_term_refs)),
                    Arc::new(StringArray::from(from_tag_refs)),
                    Arc::new(Int32Array::from(has_gclid)),
                    Arc::new(Int64Array::from(referer_hash)),
                    Arc::new(Int64Array::from(url_hash)),
                    Arc::new(Int32Array::from(clid)),
                ],
            )
            .expect("hits batch"),
        );
        offset += n;
    }

    (schema, batches)
}

// ---------------------------------------------------------------------------
// BenchmarkGenerator impl
// ---------------------------------------------------------------------------

impl BenchmarkGenerator for ClickBenchGenerator {
    fn name(&self) -> &str {
        "clickbench"
    }

    fn tables(&self) -> Vec<TableDef> {
        vec![TableDef {
            name: "hits".into(),
            schema: hits_schema(),
            row_count: |sf| (sf * ROWS_PER_SF) as usize,
        }]
    }

    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output_dir: &str,
        _config: &super::GenerateConfig,
    ) -> anyhow::Result<GenerateStats> {
        if table != "hits" {
            anyhow::bail!("Unknown ClickBench table: {table}. Only 'hits' is defined.");
        }

        let start = std::time::Instant::now();

        let (schema, batches) = generate_hits(scale);

        let full_output = format!("{output_dir}/clickbench/sf{scale}");
        let (files, bytes) =
            parquet_writer::write_parquet_files(&batches, schema, &full_output, table)?;
        let rows = batches.iter().map(|b| b.num_rows()).sum();

        Ok(GenerateStats {
            table: table.to_string(),
            rows,
            bytes: bytes as usize,
            files,
            duration: start.elapsed(),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::BenchmarkGenerator;

    #[test]
    fn test_tables_list() {
        let gen = ClickBenchGenerator;
        let tables = gen.tables();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "hits");
    }

    #[test]
    fn test_hits_schema_column_count() {
        let schema = hits_schema();
        assert_eq!(
            schema.fields().len(),
            105,
            "hits table must have exactly 105 columns"
        );
    }

    #[test]
    fn test_row_count_sf001() {
        let gen = ClickBenchGenerator;
        let sf = 0.01_f64;
        let expected = (gen.tables()[0].row_count)(sf);
        assert_eq!(expected, 1_000);
    }

    #[test]
    fn test_generate_hits_sf001() {
        let (schema, batches) = generate_hits(0.01);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1_000);
        for batch in &batches {
            assert_eq!(batch.schema(), schema);
            assert_eq!(batch.num_columns(), 105);
        }
    }

    #[test]
    fn seeded_literals_match_all_six_probed_query_patterns() {
        use arrow_array::Array as _;
        use std::collections::HashSet;

        // q19/q22/q27/q28/q39/q42 probe values a purely random generator
        // never produces; each seeded pattern must match a small but
        // nonzero fraction of rows.
        let (schema, batches) = generate_hits(0.1); // 10_000 rows
        let col = |name: &str| schema.index_of(name).unwrap();
        let uid_idx = col("UserID");
        let title_idx = col("Title");
        let url_idx = col("URL");
        let referer_idx = col("Referer");
        let phrase_idx = col("SearchPhrase");
        let counter_idx = col("CounterID");
        let utm_idx = col("UTMSource");
        let interests_idx = col("Interests");

        let mut total = 0usize;
        let mut q19_rows = 0usize;
        let mut q22_rows = 0usize;
        let mut hot_counter_rows = 0usize;
        let mut hot_referer_rows = 0usize;
        let mut utm_rows = 0usize;
        let mut hot_counter_users: HashSet<i64> = HashSet::new();

        for b in &batches {
            let uids = b
                .column(uid_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let titles = b
                .column(title_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let urls = b
                .column(url_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let referers = b
                .column(referer_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let phrases = b
                .column(phrase_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let counters = b
                .column(counter_idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let utms = b
                .column(utm_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let interests = b
                .column(interests_idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            for i in 0..b.num_rows() {
                total += 1;
                if uids.value(i) == SEEDED_USER_ID {
                    q19_rows += 1;
                }
                if titles.value(i).contains("Google")
                    && !urls.value(i).contains(".google.")
                    && !phrases.value(i).is_empty()
                {
                    q22_rows += 1;
                }
                if counters.value(i) == HOT_COUNTER_ID {
                    hot_counter_rows += 1;
                    if interests.value(i) > 0 {
                        hot_counter_users.insert(uids.value(i));
                    }
                }
                if referers.value(i).starts_with(HOT_REFERER_PREFIX) {
                    hot_referer_rows += 1;
                }
                if !utms.value(i).is_empty() {
                    utm_rows += 1;
                }
            }
        }

        assert_eq!(total, 10_000, "row count must stay scale * ROWS_PER_SF");
        // q19: WHERE "UserID" = 435090932899640449
        assert!(q19_rows > 0, "no row carries the q19 UserID literal");
        // q22: Title LIKE '%Google%' AND URL NOT LIKE '%.google.%'
        //      AND SearchPhrase <> ''
        assert!(q22_rows > 0, "no row satisfies all three q22 predicates");
        // q27: a hot CounterID concentrates rows so HAVING COUNT(*) > 100000
        // is reachable at full dataset scale (~1% of all rows)
        assert!(
            hot_counter_rows >= total / 200,
            "hot CounterID holds {hot_counter_rows} rows, expected >= 0.5%"
        );
        // q42: HAVING COUNT(DISTINCT "UserID") > 10 with "Interests" > 0
        assert!(
            hot_counter_users.len() > 10,
            "hot CounterID has only {} distinct interested users",
            hot_counter_users.len()
        );
        // q28: one referer host concentrates a hot group
        assert!(hot_referer_rows > 0, "no row uses the hot referer host");
        // q39: WHERE "UTMSource" <> ''
        assert!(utm_rows > 0, "no row has a non-empty UTMSource");
        // Seeded patterns must stay rare (< 2% of rows each)
        for (name, count) in [
            ("q19 UserID", q19_rows),
            ("q22 Google title", q22_rows),
            ("hot CounterID", hot_counter_rows),
            ("hot Referer", hot_referer_rows),
            ("UTMSource", utm_rows),
        ] {
            assert!(
                count < total / 50,
                "{name} seeded into {count} rows, expected < 2%"
            );
        }
    }

    #[test]
    fn test_hits_column_names() {
        let schema = hits_schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // Spot-check a selection of columns
        assert!(field_names.contains(&"WatchID"));
        assert!(field_names.contains(&"UserID"));
        assert!(field_names.contains(&"SearchPhrase"));
        assert!(field_names.contains(&"AdvEngineID"));
        assert!(field_names.contains(&"ResolutionWidth"));
        assert!(field_names.contains(&"MobilePhoneModel"));
        assert!(field_names.contains(&"UTMSource"));
        assert!(field_names.contains(&"URLHash"));
        assert!(field_names.contains(&"CLID"));
    }
}
