use actix_web::{get, http::header, web, App, HttpResponse, HttpServer};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::net::Ipv4Addr;
use std::{env, fmt::Debug, fs, path::Path};

// DON'T FORGET!
// postgresql://zainy@free-tier14.aws-us-east-1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full&options=--cluster%3Dtransit-plus-3127&sslrootcert=$env:appdata\CockroachCloud\certs\transit-plus-ca.crt

lazy_static! {
    static ref OFFERS: Vec<Offer> = vec![
        /*Offer {
            name: "CN Tower",
            description: "tall",
            value: 200,
            category: Category::Food,
            thumbnail: fs::read("./data/cn-tower.jpg").unwrap(),
            sales: 0,
        },*/
        Offer {
            name: "Starbucks".into(),
            description: "covfefe".into(),
            value: 100,
            category: Category::Food,
            sales: 0,
            thumbnail: "starbucks".into(),
        },
        Offer {
            name: "TTC Pass".into(),
            description: "vroom".into(),
            value: 50,
            category: Category::TTC,
            sales: 0,
            thumbnail: "pass".into(),
        },
    ];
}

const CATEGORIES: &[Category] = &[Category::Attractions, Category::Food, Category::TTC];

macro_rules! thumbnail {
    ($name:expr) => {
        Path::new("./thumbnails").join(format!("{}.jpg", $name))
    };
}

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "camelCase")]
enum Category {
    Attractions,
    Food,
    #[serde(rename = "ttc")]
    TTC,
}

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
struct Offer {
    category: Category,
    name: String,
    description: String,
    value: i64,
    sales: i64,
    thumbnail: String,
}

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
struct Img(Vec<u8>);

#[get("/listings")]
async fn listings() -> HttpResponse {
    HttpResponse::Ok()
        .content_type(header::ContentType::json())
        .body(serde_json::to_string(CATEGORIES).expect("json parse fail"))
}

#[get("/offers/{category}")]
async fn offers(req: web::Path<(Category,)>, conn: web::Data<PgPool>) -> HttpResponse {
    let (category,) = req.into_inner();
    sqlx::query_as::<_, Offer>(
        "SELECT name, description, category, value, sales, thumbnail FROM offers
        WHERE category = $1",
    )
    .bind(&category)
    .fetch_all(conn.get_ref())
    .await
    .map(|row| {
        HttpResponse::Ok()
            .content_type(header::ContentType::json())
            .body(serde_json::to_string(&row).expect("json parse fail"))
    })
    .expect("db query fail")
}

#[get("/popular")]
async fn popular(conn: web::Data<PgPool>) -> HttpResponse {
    sqlx::query_as::<_, Offer>(
        "SELECT name, description, category, value, sales, thumbnail FROM offers
        ORDER BY sales DESC
        LIMIT 10",
    )
    .fetch_all(conn.get_ref())
    .await
    .map(|row| {
        HttpResponse::Ok()
            .content_type(header::ContentType::json())
            .body(serde_json::to_string(&row).expect("json parse fail"))
    })
    .expect("db query fail")
}

#[get("/thumbnails/{name}")]
async fn thumbnails(req: web::Path<(String,)>, conn: web::Data<PgPool>) -> HttpResponse {
    let (name,) = req.into_inner();
    sqlx::query_as::<_, Img>("SELECT image FROM thumbnails WHERE name = $1")
        .bind(&name)
        .fetch_one(conn.get_ref())
        .await
        .map_or(HttpResponse::NotFound().finish(), |img| {
            HttpResponse::Ok()
                .content_type(header::ContentType::jpeg())
                .body(img.0)
        })
}

async fn load_example_db(conn: &PgPool) -> Result<(), sqlx::Error> {
    for category in CATEGORIES {
        let name = match category {
            Category::Attractions => "attractions",
            Category::Food => "food",
            Category::TTC => "ttc",
        };
        sqlx::query(
            "INSERT INTO thumbnails (name, image)
            SELECT $1, $2
            WHERE NOT EXISTS (
                SELECT name FROM thumbnails WHERE name = $1
            )",
        )
        .bind(&name)
        .bind(&fs::read(thumbnail!(&name))?)
        .execute(conn)
        .await?;
    }

    for offer in OFFERS.iter() {
        sqlx::query(
            "INSERT INTO thumbnails (name, image)
            SELECT $1, $2
            WHERE NOT EXISTS (
                SELECT name FROM thumbnails WHERE name = $1
            )",
        )
        .bind(&offer.thumbnail)
        .bind(&fs::read(thumbnail!(&offer.thumbnail))?)
        .execute(conn)
        .await?;

        sqlx::query(
            "INSERT INTO offers (name, description, value, category, sales, thumbnail)
            SELECT $1, $2, $3, $4, $5, $6
            WHERE NOT EXISTS (
                SELECT name FROM offers WHERE name = $1
            )",
        )
        .bind(&offer.name)
        .bind(&offer.description)
        .bind(&offer.value)
        .bind(&offer.category)
        .bind(&offer.sales)
        .bind(&offer.thumbnail)
        .execute(conn)
        .await?;
    }

    Ok(())
}

#[actix_web::main]
async fn main() -> Result<(), sqlx::Error> {
    println!("begin");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(
            PgConnectOptions::new()
                .application_name("transit-plus")
                .host("free-tier14.aws-us-east-1.cockroachlabs.cloud")
                .username("zainy")
                .password("g_vRaZ_exE-Go9HeIZk4JQ")
                .port(26257)
                .database("rewards")
                .options([("cluster", "transit-plus-3127")]), //.ssl_mode(PgSslMode::VerifyFull)
                                                              //.ssl_root_cert("./certs/transit-plus-ca.crt"),
        )
        .await?;

    //load_example_db(&pool).await?;

    println!("{}", env::var("PORT").unwrap());

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .service(listings)
            .service(offers)
            .service(popular)
            .service(thumbnails)
    })
    .bind((
        Ipv4Addr::new(127, 0, 0, 1),
        env::var("PORT")
            .unwrap_or(String::from("3000"))
            .parse()
            .expect("$PORT must be a number"),
    ))?
    .run()
    .await?;

    Ok(())
}
