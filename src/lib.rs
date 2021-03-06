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
extern crate chrono;
extern crate chrono_tz;
use chrono::{prelude::*, DateTime, Utc};

use std::fmt;
//use postgres::{Connection, TlsMode};

enum Operator {
    Equal,
    NotEqual,
    LT,
    LTE,
    GT,
    GTE,
    Add,
    Subtract,
    Multiply,
    OR,
    AND,
    IN
}

impl FromStr for Operator {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        match s.as_ref() {
            "==" => Ok(Operator::Equal),
            "!=" => Ok(Operator::NotEqual),
            "<" => Ok(Operator::LT),
            "<=" => Ok(Operator::LTE),
            ">" => Ok(Operator::GT),
            ">=" => Ok(Operator::GTE),
            "+" => Ok(Operator::Add),
            "-" => Ok(Operator::Subtract),
            "*" => Ok(Operator::Multiply),
            "or" | "OR" => Ok(Operator::OR),
            "and" | "AND" => Ok(Operator::AND),
            "in" | "IN" => Ok(Operator::IN),
            x => Err(err_msg(format!("operator_not_found: {}", x))),
        }
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Operator::Equal => write!(f, "=="),
            Operator::NotEqual => write!(f, "!="),
            Operator::LT => write!(f, "<"),
            Operator::LTE => write!(f, "<="),
            Operator::GT => write!(f, ">"),
            Operator::GTE => write!(f, ">="),
            Operator::Add => write!(f, "+"),
            Operator::Subtract => write!(f, "-"),
            Operator::Multiply => write!(f, "*"),
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
    VecDateTime(Vec<DateTime<Utc>>),
    VecString(Vec<String>),
    ValInVec(serde_json::Value, Vec<serde_json::Value>),
}

impl ValueList {
    fn lt(&self) -> Result<bool, Error>
    {
        match self {
            ValueList::Veci32(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a < b)),
            ValueList::Vecf64(d) =>Ok(d.into_iter().tuple_windows().all(|(a, b)| a < b)),
            ValueList::VecDateTime(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a.signed_duration_since(*b).num_days()<0)),
//            ValueList::VecString(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a < b)),
            _ => Err(err_msg(format!("invalid_data_type {:?} for operator {} ", &self, Operator::LTE)))
        }
    }
    fn lte(&self) -> Result<bool, Error>
    {
        match self {
            ValueList::Veci32(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a <= b)),
            ValueList::Vecf64(d) =>Ok(d.into_iter().tuple_windows().all(|(a, b)| a <= b)),
            ValueList::VecDateTime(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a.signed_duration_since(*b).num_days()<=0)),
//            ValueList::VecString(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a <= b)),
            _ => Err(err_msg(format!("invalid_data_type {:?} for operator {} ", &self, Operator::LTE)))
        }
    }
    fn gt(&self) -> Result<bool, Error>
    {
        match self {
            ValueList::Veci32(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a > b)),
            ValueList::Vecf64(d) =>Ok(d.into_iter().tuple_windows().all(|(a, b)| a > b)),
            ValueList::VecDateTime(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a.signed_duration_since(*b).num_days()>0)),
//            ValueList::VecString(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a > b)),
            _ => Err(err_msg(format!("invalid_data_type {:?} for operator {} ", &self, Operator::GT)))
        }
    }
    fn gte(&self) -> Result<bool, Error>
    {
        match self {
            ValueList::Veci32(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a >= b)),
            ValueList::Vecf64(d) =>Ok(d.into_iter().tuple_windows().all(|(a, b)| a >= b)),
            ValueList::VecDateTime(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a.signed_duration_since(*b).num_days()>=0)),
//            ValueList::VecString(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a >= b)),
            _ => Err(err_msg(format!("invalid_data_type {:?} for operator {} ", &self, Operator::GTE)))
        }
    }

    fn add(&self) -> Result<serde_json::Value, Error>
    {
        match self {
            ValueList::Veci32(d) => {
                let x: i32 = d.iter().sum();
                Ok(serde_json::Value::from(x))
            },
            ValueList::Vecf64(d) => {
                let x: f64 = d.iter().sum();
                Ok(serde_json::Value::from(x))
            },
            _ => Err(err_msg(format!("invalid_data_type {:?} for operator {} ", &self, Operator::Add)))
        }
    }

    fn multiply(&self) -> Result<serde_json::Value, Error>
    {
        match self {
            ValueList::Veci32(d) => {
                let mut x = 1;
                let mut i = d.iter();
                for v in i {
                    x = x*(*v);
                }
                return Ok(serde_json::Value::from(x));
            },
            ValueList::Vecf64(d) => {
                let mut x = 1.0;
                let mut i = d.iter();
                for v in i {
                    x = x*(*v);
                }
                return Ok(serde_json::Value::from(x));
            },
            _ => Err(err_msg(format!("invalid_data_type {:?} for operator {} ", &self, Operator::Multiply)))
        }
    }

    fn subtract(&self) -> Result<serde_json::Value, Error>
    {
        match self {
            ValueList::Veci32(d) => {
                let mut x = *(d.first().unwrap_or(&0));
                let mut i = d.iter();
                i.next();
                for v in i {
                    x = x-*v;
                }
                return Ok(serde_json::Value::from(x));
            },
            ValueList::Vecf64(d) => {
                let mut x = *(d.first().unwrap_or(&0.0));
                let mut i = d.iter();
                i.next();
                for v in i {
                    x = x-*v;
                }
                return Ok(serde_json::Value::from(x));
            },
            _ => Err(err_msg(format!("invalid_data_type {:?} for operator {} ", &self, Operator::Subtract)))
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
            ValueList::ValInVec(a,b) => {
                let mut x = (*a).clone();
                if !x.is_array() {
                    x = serde_json::Value::Array(vec![x]);
                }
                Ok(b.contains(&x) || b.contains(a))
            },
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
        println!("{:?}", a);
        match self {
            Operator::Equal => Ok(serde_json::Value::Bool(rule.iter().all_equal())),
            Operator::NotEqual => Ok(serde_json::Value::Bool(!rule.iter().all_equal())),
            Operator::LT => {
                let lte = a.lt()?;
                Ok(serde_json::Value::Bool(lte))
            },
            Operator::LTE => {
                let lte = a.lte()?;
                Ok(serde_json::Value::Bool(lte))
            },
            Operator::GT => {
                let lte = a.gt()?;
                Ok(serde_json::Value::Bool(lte))
            },
            Operator::GTE => {
                let lte = a.gte()?;
                Ok(serde_json::Value::Bool(lte))
            },
            Operator::Add => {
                a.add()
            },
            Operator::Multiply => {
                a.multiply()
            },
            Operator::Subtract => {
                let s = a.subtract()?;
                Ok(s)
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

pub fn parse_year_now(s: &String) -> Result<i32, Error> {
    if s.starts_with("NOW.YEAR") {
        let mut s1 = s.clone();
        s1.retain(|c|c!=' ');
        if s1.as_str() == "NOW" {
            return Ok(Utc::now().month() as i32);
        } else if s1.as_str().contains("+") {
            let parts = s1.split("+").collect::<Vec<_>>();
            if parts.len()!=2 {
                return Err(err_msg("parse_time_err"));
            }
            let d = parts[1].parse::<i32>().unwrap();
            return Ok(Utc::now().year() + d)
        }
        if s1.as_str().contains("-") {
            let parts = s1.split("-").collect::<Vec<_>>();
            if parts.len()!=2 {
                return Err(err_msg("parse_time_err"));
            }
            let d = parts[1].parse::<i32>().unwrap();
            return Ok(Utc::now().year() - d)
        }
    }
    return Err(err_msg("parse_year_err"));
}

pub fn parse_month_now(s: &String) -> Result<i32, Error> {
    if s.starts_with("NOW.MONTH") {
        let mut s1 = s.clone();
        s1.retain(|c|c!=' ');
        if s1.as_str() == "NOW" {
            return Ok(Utc::now().month() as i32);
        } else if s1.as_str().contains("+") {
            let parts = s1.split("+").collect::<Vec<_>>();
            if parts.len()!=2 {
                return Err(err_msg("parse_time_err"));
            }
            let d = parts[1].parse::<i32>().unwrap();
            return Ok(Utc::now().month() as i32 + d)
        }
        if s1.as_str().contains("-") {
            let parts = s1.split("-").collect::<Vec<_>>();
            if parts.len()!=2 {
                return Err(err_msg("parse_time_err"));
            }
            let d = parts[1].parse::<i32>().unwrap();
            return Ok(Utc::now().month() as i32 - d)
        }
    }
    return Err(err_msg("parse_year_err"));
}

pub fn parse_time_now(s: &String) -> Result<DateTime<Utc>, Error> {
    if s.starts_with("NOW") {
        let mut s1 = s.clone();
        s1.retain(|c|c!=' ');
        if s1.as_str() == "NOW" {
            return Ok(Utc::now());
        } else if s1.as_str().contains("+") {
            let parts = s1.split("+").collect::<Vec<_>>();
            if parts.len()!=2 {
                return Err(err_msg("parse_time_err"));
            }
            let d = parts[1].parse::<i64>().unwrap();
            return Ok(Utc::now() + chrono::Duration::days(d));
        }
        if s1.as_str().contains("-") {
            let parts = s1.split("-").collect::<Vec<_>>();
            if parts.len()!=2 {
                return Err(err_msg("parse_time_err"));
            }
            let d = parts[1].parse::<i64>().unwrap();
            return Ok(Utc::now() - chrono::Duration::days(d));
        }
    }
    return Err(err_msg("parse_time_err"));
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
            let mut keys = o.keys();
            let mut x = keys.next().ok_or(err_msg("Empty Map"))?;
            if x == "err" {
                x = keys.next().ok_or(err_msg("Empty Map"))?;
            }
//            let err: Option<&str> = o.keys().next().and_then(|k| o.get(k)).and_then(|v| v.as_str());
            let err = o.get("err").and_then(|v|v.as_str());
            println!("inside_object: x: {:?}", x);
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
                Ok(serde_json::Value::Array(parse_sql_query(&s, conn)?))
            } else if s.starts_with("NOW.YEAR") {
                Ok(serde_json::Value::from(parse_year_now(s)?))
            } else if s.starts_with("NOW.MONTH") {
                Ok(serde_json::Value::from(parse_month_now(s)?))
            } else if s.starts_with("NOW") {
                Ok(serde_json::Value::String(parse_time_now(s)?.to_rfc3339()))
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
        "err": "My err"}), &json!({ "a": 1, "b": 2 }), Some(&conn)).unwrap();
        let y = super::eval(&json!({"in": ["bike_comprehensive", "select id from ackore_plan"],
        "err": "My err"}), &json!({ "a": 1, "b": 2 }), Some(&conn)).unwrap();

        let y = super::eval(&json!({">=": ["2019-05-03T00:00:00+00:00", "NOW-10"]}), &json!({ "a": 1, "b": 2 }), Some(&conn)).unwrap();
        assert_eq!(y, serde_json::Value::Bool(true));
        let y = super::eval(&json!({"*": [8.0, 3]}), &json!({ "a": 1, "b": 2 }), Some(&conn)).unwrap();
        assert_eq!(y, serde_json::Value::from(24.0));

//        let s_json_agg = "select json_agg(t) as output from (select * from masters_intermediaryr`toplanmapping limit 1) t";
//        let a: Vec<super::Output> = sql_query(s_json_agg).load(&conn).unwrap();
    }
}
