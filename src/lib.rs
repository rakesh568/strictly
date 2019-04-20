#![recursion_limit = "128"]
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate diesel;
extern crate itertools;
#[macro_use]
extern crate serde_json;
use diesel::{prelude::*, sql_query, PgConnection};
use failure::{err_msg, Error};
use itertools::Itertools;
use std::ops::Add;
use std::str::FromStr;
extern crate postgres;
use postgres::{Connection, TlsMode};

pub enum Operator {
    Equal,
    LTE,
    OR,
    AND
}

#[derive(PartialEq, Debug, Serialize, Deserialize, PartialOrd, Clone)]
#[serde(untagged)]
pub enum ValueList {
    VecBool(Vec<bool>),
    Veci32(Vec<i32>),
    Vecf64(Vec<f64>),
    VecString(Vec<String>),
    I32inVec(i32, Vec<i32>),
    F64inVec(f64, Vec<f64>),
    StringInVec(String, Vec<String>),
}

impl ValueList {
    fn lte(&self) -> Result<bool, Error>
    {
        match self {
            ValueList::Veci32(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a <= b)),
            ValueList::Vecf64(d) =>Ok(d.into_iter().tuple_windows().all(|(a, b)| a <= b)),
            ValueList::VecString(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| a <= b)),
            _ => Err(err_msg("invalid_data_type"))
        }
    }
    fn or(&self) -> Result<bool, Error> {
        match self {
            ValueList::VecBool(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| *a || *b)),
            _ => Err(err_msg("invalid_data_type"))
        }
    }
    fn and(&self) -> Result<bool, Error> {
        match self {
            ValueList::VecBool(d) => Ok(d.into_iter().tuple_windows().all(|(a, b)| *a && *b)),
            _ => Err(err_msg("invalid_data_type"))

        }
    }
}

pub trait Operation<T> {
    fn operate(&self, rule: Vec<T>) -> Result<serde_json::Value, failure::Error>;
}

impl Operation<serde_json::Value> for Operator {
    fn operate(&self, rule: Vec<serde_json::Value>) -> Result<serde_json::Value, failure::Error> {
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
                let or = a.or()?;
                Ok(serde_json::Value::Bool(or))
            },
            _ => Ok(serde_json::Value::Bool(true)),
        }
    }
}

impl FromStr for Operator {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        match s.as_ref() {
            "==" => Ok(Operator::Equal),
            "<=" => Ok(Operator::LTE),
            "OR" => Ok(Operator::OR),
            x => Err(err_msg(format!("operator_not_found: {}", x))),
        }
    }
}

pub fn eval(
    rule: &serde_json::Value,
    data: &serde_json::Value,
    conn: Option<&Connection>,
) -> Result<serde_json::Value, Error> {
    let d = data.as_object().unwrap();
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
            let op = Operator::from_str(&x)?;
            let v = o.get(&x.clone()).ok_or(err_msg(format!("no_value_found for {}", x)))?;
            let v: Vec<serde_json::Value> = serde_json::from_value( eval(v, data, conn)?)?;
            Ok(op.operate(v)?)
        }
        serde_json::Value::String(s) => {
            if s.starts_with("$") {
                let k = &s.clone()[1..];
                data.get(k)
                    .map(|x| (*x).clone())
                    .ok_or(err_msg(format!("var_not_found {}", s)))
            } else if s.starts_with("SELECT") || s.starts_with("select") {
                for row in &conn.unwrap().query("SELECT values FROM table1", &[]).unwrap() {
                    let val: serde_json::Value = row.get(0);
                }
                Ok(serde_json::Value::Array(a.unwrap()))
            } else {
                Ok(serde_json::Value::String(s.clone()))
            }
        }
        _ => Ok(rule.clone()),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test1() {
        let x = super::eval(&json!({ "==" : [true, {"OR": [true, {"<=": ["$b", 3]}]}] }), &json!({ "a": 1, "b": 2 }), None).unwrap();
        assert_eq!(x, serde_json::Value::Bool(true));
    }
}
