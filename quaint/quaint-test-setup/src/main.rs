use quaint::{ast::Insert, prelude::Queryable, single::Quaint};
// use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), quaint::error::Error> {
    let full_sql = "INSERT INTO \"public\".\"user\" (\"id\",\"email\",\"deleted\",\"createdAt\") VALUES ('edbe0722f5bf40d7a2602cbf4ab5d943', 'Rowena.Wiza@yahoo.com', false, null) RETURNING \"public\".\"user\".\"id\", \"public\".\"user\".\"email\", \"public\".\"user\".\"deleted\", \"public\".\"user\".\"createdAt\"";
    let conn = Quaint::new("postgres://root:root@localhost:3307/demo_ds").await?;
    // let conn = Quaint::new("postgres://postgres:postgres@localhost:5432/demo_ds").await?;
    let del_sql = "delete from \"public\".\"user\"";
    conn.raw_cmd(&del_sql).await?;
    let ret = conn.query_raw(&full_sql, &[]).await?;
    println!("ok");
    // let re = regex::Regex::new(r"\?").unwrap();
    // let mut matches = re.find_iter(&full_sql);
    // let sqls: Vec<_> = full_sql.split('?').collect();
    // let mut idx = 0;
    // let vals: Vec<_> = ["'0011df048ec64567abd3c20ef2365a4c'"].to_vec();
    // let mut sb = String::new();
    // for match_ in matches {
    //     println!("start:{}", match_.start());
    //     println!("end:{}", match_.end());
    //     sb.push_str(sqls.get(idx).unwrap());
    //     sb.push_str(vals.get(idx).unwrap());
    //     idx += 1;
    // }
    // sb.push_str(sqls.get(idx).unwrap());
    // println!("最终替换的sql:{}", sb);
    // let pool = mysql_async::Pool::new(
    //     "mysql://root:5d12925679157bfda4d97dbcb61e0885@10.3.250.9:9030/neimeng_statistics",
    // );
    // let mut conn = pool.get_conn().await?;
    // let results: Vec<(Vec<u8>)> = conn.query(&sb).await.unwrap();
    // for r in results {
    //     let x = parse_mysql_datetime_string(&r);
    //     println!("{:?}", x);
    // }
    // conn.query_iter(sb).await.unwrap().for_each(|row| {
    //     let r: (NaiveDateTime) = from_row(row);
    //     println!("{:?}", r);
    // }).await;
    // drop(conn);
    // pool.disconnect().await?;
    Ok(())
}
