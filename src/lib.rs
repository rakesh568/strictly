#![recursion_limit = "128"]
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate diesel;
extern crate itertools;
#[macro_use]
extern crate serde_json;
use diesel::{prelude::*, sql_query, PgConnection, RunQueryDsl, sql_types::Json};
use failure::{err_msg, Error};
use itertools::Itertools;
use std::ops::Add;
use std::str::FromStr;
extern crate postgres;
extern crate slog;
extern crate slog_scope;
use std::fmt;
use postgres::{Connection, TlsMode};

enum Operator {
    Equal,
    LT,
    LTE,
    GT,
    GTE,
    OR,
    AND,
    IN
}

impl FromStr for Operator {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        match s.as_ref() {
            "==" => Ok(Operator::Equal),
            "<" => Ok(Operator::LT),
            "<=" => Ok(Operator::LTE),
            ">" => Ok(Operator::GT),
            ">=" => Ok(Operator::GTE),
            "or" | "OR" => Ok(Operator::OR),
            "in" | "IN" => Ok(Operator::IN),
            x => Err(err_msg(format!("operator_not_found: {}", x))),
        }
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Operator::Equal => write!(f, "=="),
            Operator::LT => write!(f, "<"),
            Operator::LTE => write!(f, "<="),
            Operator::GT => write!(f, ">"),
            Operator::GTE => write!(f, ">="),
            Operator::OR => write!(f, "or"),
            Operator::AND => write!(f, "and"),
            Operator::IN => write!(f, "in"),
        }
    }
}

#[derive(Debug, Deserialize, QueryableByName)]
struct Output {
    #[sql_type = "Json"]
    pub output: serde_json::Value,
}


#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum ValueList {
    VecBool(Vec<bool>),
    Veci32(Vec<i32>),
    Vecf64(Vec<f64>),
    VecString(Vec<String>),
    ValInVec(serde_json::Value, Vec<serde_json::Value>),
}

impl ValueList {
    fn lte(&self) -> Result<bool, Error>
    {
        match self {
            ValueList::Veci32(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a <= b)),
            ValueList::Vecf64(d) =>Ok(d.into_iter().tuple_windows().all(|(a, b)| a <= b)),
            ValueList::VecString(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a <= b)),
            _ => Err(err_msg(format!("invalid_data_type {:?} for operator {} ", &self, Operator::LTE)))
        }
    }
    fn or(&self) -> Result<bool, Error> {
        match self {
            ValueList::VecBool(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| *a || *b)),
            _ => Err(err_msg(format!("invalid_data_type {:?} for or {}", &self, Operator::OR)))
        }
    }
    fn and(&self) -> Result<bool, Error> {
        match self {
            ValueList::VecBool(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| *a && *b)),
            _ => Err(err_msg("invalid_data_type"))

        }
    }
    fn in_(&self) -> Result<bool, Error> {
        match self {
            ValueList::ValInVec(a,b) => Ok(b.contains(a)),
            _ => Err(err_msg("invalid_data_type"))

        }
    }
}

trait Operation<T> {
    fn operate(&self, rule: &Vec<T>) -> Result<serde_json::Value, failure::Error>;
}

impl Operation<serde_json::Value> for Operator {
    fn operate(&self, rule: &Vec<serde_json::Value>) -> Result<serde_json::Value, failure::Error> {
        let v = serde_json::to_value(rule.clone())?;
        let a: ValueList = serde_json::from_value(v)?;
        match self {
            Operator::Equal => Ok(serde_json::Value::Bool(rule.iter().all_equal())),
            Operator::LTE => {
                let lte = a.lte()?;
                Ok(serde_json::Value::Bool(lte))
            },
            Operator::OR => {
                let or = a.or()?;
                Ok(serde_json::Value::Bool(or))
            },
            Operator::AND => {
                let or = a.and()?;
                Ok(serde_json::Value::Bool(or))
            },
            Operator::IN => {
                let in_ = a.in_()?;
                Ok(serde_json::Value::Bool(in_))
            },
            _ => Err(err_msg(format!("Unsupported operator"))),
        }
    }
}

fn json_aggregate(s: &String) -> String {
    let a = "select json_agg(t) as output from (".to_owned();
    let b = ") t";
    a+&s+b
}

fn parse_sql_query(s: &String, conn: &PgConnection) -> Result<Vec<serde_json::Value>, Error> {
    let s_json_agg = json_aggregate(s);
    println!("");
    let a: Vec<Output> = sql_query(s_json_agg).load(conn)
                    .map_err(|_|err_msg(format!("query_err: {}", s)))?;
    let a = match &a[0].output {
        serde_json::Value::Array(x) => x,
        _ => return Err(err_msg("query_err"))
    };
    let mut out: Vec<serde_json::Value> = vec![];
    for i in a.iter() {
        let m = i.as_object().ok_or(err_msg(format!("query_parse_err: {}", s)))?;
        let m: Vec<serde_json::Value> = m.values().cloned().collect();
        out.push(serde_json::Value::from(m))
    }
    println!("{:?}", out);
    Ok(out)
}

pub fn eval(
    rule: &serde_json::Value,
    data: &serde_json::Value,
    conn: Option<&PgConnection>,
) -> Result<serde_json::Value, Error> {
    let d = data.as_object().ok_or(err_msg("Input data should be serde_json::Value::Object"))?;
    match rule {
        serde_json::Value::Array(a) => {
            let mut evaled = vec![];
            for v in a.iter() {
                evaled.push(eval(v, data, conn)?)
            }
            Ok(serde_json::Value::Array(evaled))
        }
        serde_json::Value::Object(o) => {
            let x = o.keys().next().unwrap();
//            let err: Option<&str> = o.keys().next().and_then(|k| o.get(k)).and_then(|v| v.as_str());
            let err = o.get("err").and_then(|v|v.as_str());

            let op = Operator::from_str(&x)?;
            let v = o.get(&x.clone()).ok_or(err_msg(format!("no_value_found for {}", x)))?;
            let v: Vec<serde_json::Value> = serde_json::from_value( eval(v, data, conn)?)?;
            let out = op.operate(&v);
            match out {
                Ok(serde_json::Value::Bool(false)) => match err {
                    Some(e) => Err(err_msg(format!("{}", e))),
                    _ => Err(err_msg(format!("{:?} should be {} {:?}", &v.first(), op, &v.get(1..)))),
                },
                _ => out
            }
        }
        serde_json::Value::String(s) => {
            if s.starts_with("$") {
                let k = &s.clone()[1..];
                data.get(k)
                    .map(|x| (*x).clone())
                    .ok_or(err_msg(format!("var_not_found {}", s)))
            } else if s.starts_with("SELECT") || s.starts_with("select") {
                let conn = conn.ok_or(err_msg("Need connection for executing sql query"))?;
                Ok(serde_json::Value::Array(parse_sql_query(s, conn)?))
            } else {
                Ok(serde_json::Value::String(s.clone()))
            }
        }
        _ => Ok(rule.clone()),
    }
}

#[cfg(test)]
mod tests {
    use diesel::{prelude::*, sql_query, PgConnection, RunQueryDsl, sql_types::Jsonb};
    use crate::diesel::Connection;
    #[test]
    fn test1() {
        let db_url = "postgres://root@127.0.0.1/acko";
        let conn: PgConnection = Connection::establish(&db_url).expect("No connection established");

//        let x = super::eval(&json!({ "==" : [true, {"OR": [true, {"<=": ["$registration_year", 2016]}]}] }), &json!({ "a": 1, "registration_year": 2018 }), Some(&conn)).unwrap();
//        assert_eq!(x, serde_json::Value::Bool(true));
        let y = super::eval(&json!({"in": [[35, "Invictus Insurance Broking Services Private Limited"], "select id, name from masters_intermediary"],
        "err": ""}), &json!({ "a": 1, "b": 2 }), Some(&conn)).unwrap();
        assert_eq!(y, serde_json::Value::Bool(true));
//        let s_json_agg = "select json_agg(t) as output from (select * from masters_intermediaryrtoplanmapping limit 1) t";
//        let a: Vec<super::Output> = sql_query(s_json_agg).load(&conn).unwrap();
    }
}
