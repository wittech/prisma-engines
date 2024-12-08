use crate::{
    ast::{Value, ValueType},
    connector::{queryable::TakeRow, TypeIdentifier},
    error::{Error, ErrorKind},
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use lexical::parse;
use mysql_async::{
    self as my,
    consts::{ColumnFlags, ColumnType},
};
use regex::bytes::Regex;
use std::convert::TryFrom;
lazy_static::lazy_static! {
    static ref DATETIME_RE_YMD: Regex = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
    static ref DATETIME_RE_YMD_HMS: Regex =
        Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$").unwrap();
    static ref DATETIME_RE_YMD_HMS_NS: Regex =
        Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{1,6}$").unwrap();
    static ref TIME_RE_HH_MM_SS: Regex = Regex::new(r"^\d{2}:[0-5]\d:[0-5]\d$").unwrap();
    static ref TIME_RE_HH_MM_SS_MS: Regex =
        Regex::new(r"^\d{2}:[0-5]\d:[0-5]\d\.\d{1,6}$").unwrap();
    static ref TIME_RE_HHH_MM_SS: Regex = Regex::new(r"^[0-8]\d\d:[0-5]\d:[0-5]\d$").unwrap();
    static ref TIME_RE_HHH_MM_SS_MS: Regex =
        Regex::new(r"^[0-8]\d\d:[0-5]\d:[0-5]\d\.\d{1,6}$").unwrap();
}
pub fn parse_mysql_datetime_string(bytes: &[u8]) -> Option<(u32, u32, u32, u32, u32, u32, u32)> {
    let len = bytes.len();

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    #[repr(u8)]
    enum DateTimeKind {
        Ymd = 0,
        YmdHms,
        YmdHmsMs,
    }

    let kind = if len == 10 && DATETIME_RE_YMD.is_match(bytes) {
        DateTimeKind::Ymd
    } else if len == 19 && DATETIME_RE_YMD_HMS.is_match(bytes) {
        DateTimeKind::YmdHms
    } else if 20 < len && len < 27 && DATETIME_RE_YMD_HMS_NS.is_match(bytes) {
        DateTimeKind::YmdHmsMs
    } else {
        return None;
    };

    let (year, month, day, hour, minute, second, micros) = match kind {
        DateTimeKind::Ymd => (..4, 5..7, 8..10, None, None, None, None),
        DateTimeKind::YmdHms => (..4, 5..7, 8..10, Some(11..13), Some(14..16), Some(17..19), None),
        DateTimeKind::YmdHmsMs => (..4, 5..7, 8..10, Some(11..13), Some(14..16), Some(17..19), Some(20..)),
    };

    Some((
        parse(&bytes[year]).unwrap(),
        parse(&bytes[month]).unwrap(),
        parse(&bytes[day]).unwrap(),
        hour.map(|pos| parse(&bytes[pos]).unwrap()).unwrap_or(0),
        minute.map(|pos| parse(&bytes[pos]).unwrap()).unwrap_or(0),
        second.map(|pos| parse(&bytes[pos]).unwrap()).unwrap_or(0),
        micros.map(|pos| parse_micros(&bytes[pos])).unwrap_or(0),
    ))
}

fn parse_micros(micros_bytes: &[u8]) -> u32 {
    let mut micros = parse(micros_bytes).unwrap();

    let mut pad_zero_cnt = 0;
    for b in micros_bytes.iter() {
        if *b == b'0' {
            pad_zero_cnt += 1;
        } else {
            break;
        }
    }

    for _ in 0..(6 - pad_zero_cnt - (micros_bytes.len() - pad_zero_cnt)) {
        micros *= 10;
    }
    micros
}

pub fn parse_mysql_time_string(mut bytes: &[u8]) -> Option<(bool, u32, u32, u32, u32)> {
    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    #[repr(u8)]
    enum TimeKind {
        HhMmSs = 0,
        HhhMmSs,
        HhMmSsMs,
        HhhMmSsMs,
    }

    if bytes.len() < 8 {
        return None;
    }

    let is_neg = bytes[0] == b'-';
    if is_neg {
        bytes = &bytes[1..];
    }

    let len = bytes.len();

    let kind = if len == 8 && TIME_RE_HH_MM_SS.is_match(bytes) {
        TimeKind::HhMmSs
    } else if len == 9 && TIME_RE_HHH_MM_SS.is_match(bytes) {
        TimeKind::HhhMmSs
    } else if TIME_RE_HH_MM_SS_MS.is_match(bytes) {
        TimeKind::HhMmSsMs
    } else if TIME_RE_HHH_MM_SS_MS.is_match(bytes) {
        TimeKind::HhhMmSsMs
    } else {
        return None;
    };

    let (hour_pos, min_pos, sec_pos, micros_pos) = match kind {
        TimeKind::HhMmSs => (..2, 3..5, 6..8, None),
        TimeKind::HhMmSsMs => (..2, 3..5, 6..8, Some(9..)),
        TimeKind::HhhMmSs => (..3, 4..6, 7..9, None),
        TimeKind::HhhMmSsMs => (..3, 4..6, 7..9, Some(10..)),
    };

    Some((
        is_neg,
        parse(&bytes[hour_pos]).unwrap(),
        parse(&bytes[min_pos]).unwrap(),
        parse(&bytes[sec_pos]).unwrap(),
        micros_pos.map(|pos| parse_micros(&bytes[pos])).unwrap_or(0),
    ))
}

//todo：将变量值转换为简单类型数组
pub fn conv_params_simple(params: &[Value<'_>]) -> crate::Result<Vec<mysql_async::Value>> {
    if params.is_empty() {
        // If we don't use explicit 'Empty',
        // mysql crashes with 'internal error: entered unreachable code'
        Ok(Vec::with_capacity(0))
    } else {
        let mut values = Vec::with_capacity(params.len());

        for pv in params {
            let res = match &pv.typed {
                ValueType::Int32(i) => i.map(|i| my::Value::Int(i as i64)),
                ValueType::Int64(i) => i.map(my::Value::Int),
                ValueType::Float(f) => f.map(my::Value::Float),
                ValueType::Double(f) => f.map(my::Value::Double),
                ValueType::Text(s) => s.clone().map(|s| my::Value::Bytes((*s).as_bytes().to_vec())),
                ValueType::Bytes(bytes) => bytes.clone().map(|bytes| my::Value::Bytes(bytes.into_owned())),
                ValueType::Enum(s, _) => s.clone().map(|s| my::Value::Bytes((*s).as_bytes().to_vec())),
                ValueType::Boolean(b) => b.map(|b| my::Value::Int(b as i64)),
                ValueType::Char(c) => c.map(|c| my::Value::Bytes(vec![c as u8])),
                ValueType::Xml(s) => s.as_ref().map(|s| my::Value::Bytes((s).as_bytes().to_vec())),
                ValueType::Array(_) | ValueType::EnumArray(_, _) => {
                    let msg = "Arrays are not supported in MySQL.";
                    let kind = ErrorKind::conversion(msg);

                    let mut builder = Error::builder(kind);
                    builder.set_original_message(msg);

                    return Err(builder.build());
                }

                ValueType::Numeric(f) => f.as_ref().map(|f| my::Value::Bytes(f.to_string().as_bytes().to_vec())),
                ValueType::Json(s) => match s {
                    Some(ref s) => {
                        let json = serde_json::to_string(s)?;
                        let bytes = json.into_bytes();

                        Some(my::Value::Bytes(bytes))
                    }
                    None => None,
                },
                ValueType::Uuid(u) => u.map(|u| my::Value::Bytes(u.hyphenated().to_string().into_bytes())),
                ValueType::Date(d) => {
                    d.map(|d| my::Value::Date(d.year() as u16, d.month() as u8, d.day() as u8, 0, 0, 0, 0))
                }
                ValueType::Time(t) => {
                    t.map(|t| my::Value::Time(false, 0, t.hour() as u8, t.minute() as u8, t.second() as u8, 0))
                }
                ValueType::DateTime(dt) => dt.map(|dt| {
                    my::Value::Date(
                        dt.year() as u16,
                        dt.month() as u8,
                        dt.day() as u8,
                        dt.hour() as u8,
                        dt.minute() as u8,
                        dt.second() as u8,
                        dt.timestamp_subsec_micros(),
                    )
                }),
            };

            match res {
                Some(val) => values.push(val),
                None => values.push(my::Value::NULL),
            }
        }

        Ok(values)
    }
}

pub fn conv_params(params: &[Value<'_>]) -> crate::Result<my::Params> {
    if params.is_empty() {
        // If we don't use explicit 'Empty',
        // mysql crashes with 'internal error: entered unreachable code'
        Ok(my::Params::Empty)
    } else {
        let mut values = Vec::with_capacity(params.len());

        for pv in params {
            let res = match &pv.typed {
                ValueType::Int32(i) => i.map(|i| my::Value::Int(i as i64)),
                ValueType::Int64(i) => i.map(my::Value::Int),
                ValueType::Float(f) => f.map(my::Value::Float),
                ValueType::Double(f) => f.map(my::Value::Double),
                ValueType::Text(s) => s.clone().map(|s| my::Value::Bytes((*s).as_bytes().to_vec())),
                ValueType::Bytes(bytes) => bytes.clone().map(|bytes| my::Value::Bytes(bytes.into_owned())),
                ValueType::Enum(s, _) => s.clone().map(|s| my::Value::Bytes((*s).as_bytes().to_vec())),
                ValueType::Boolean(b) => b.map(|b| my::Value::Int(b as i64)),
                ValueType::Char(c) => c.map(|c| my::Value::Bytes(vec![c as u8])),
                ValueType::Xml(s) => s.as_ref().map(|s| my::Value::Bytes((s).as_bytes().to_vec())),
                ValueType::Array(_) | ValueType::EnumArray(_, _) => {
                    let msg = "Arrays are not supported in MySQL.";
                    let kind = ErrorKind::conversion(msg);

                    let mut builder = Error::builder(kind);
                    builder.set_original_message(msg);

                    return Err(builder.build());
                }

                ValueType::Numeric(f) => f.as_ref().map(|f| my::Value::Bytes(f.to_string().as_bytes().to_vec())),
                ValueType::Json(s) => match s {
                    Some(ref s) => {
                        let json = serde_json::to_string(s)?;
                        let bytes = json.into_bytes();

                        Some(my::Value::Bytes(bytes))
                    }
                    None => None,
                },
                ValueType::Uuid(u) => u.map(|u| my::Value::Bytes(u.hyphenated().to_string().into_bytes())),
                ValueType::Date(d) => {
                    d.map(|d| my::Value::Date(d.year() as u16, d.month() as u8, d.day() as u8, 0, 0, 0, 0))
                }
                ValueType::Time(t) => {
                    t.map(|t| my::Value::Time(false, 0, t.hour() as u8, t.minute() as u8, t.second() as u8, 0))
                }
                ValueType::DateTime(dt) => dt.map(|dt| {
                    my::Value::Date(
                        dt.year() as u16,
                        dt.month() as u8,
                        dt.day() as u8,
                        dt.hour() as u8,
                        dt.minute() as u8,
                        dt.second() as u8,
                        dt.timestamp_subsec_micros(),
                    )
                }),
            };

            match res {
                Some(val) => values.push(val),
                None => values.push(my::Value::NULL),
            }
        }

        Ok(my::Params::Positional(values))
    }
}

impl TypeIdentifier for &my::Column {
    fn is_real(&self) -> bool {
        use ColumnType::*;

        matches!(self.column_type(), MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL)
    }

    fn is_float(&self) -> bool {
        use ColumnType::*;

        matches!(self.column_type(), MYSQL_TYPE_FLOAT)
    }

    fn is_double(&self) -> bool {
        use ColumnType::*;

        matches!(self.column_type(), MYSQL_TYPE_DOUBLE)
    }

    fn is_int32(&self) -> bool {
        use ColumnType::*;

        let is_unsigned = self.flags().intersects(ColumnFlags::UNSIGNED_FLAG);

        // https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary
        // MYSQL_TYPE_TINY  = i8
        // MYSQL_TYPE_SHORT = i16
        // MYSQL_TYPE_YEAR  = i16
        // SIGNED MYSQL_TYPE_LONG  = i32
        // SIGNED MYSQL_TYPE_INT24 = i32
        matches!(
            (self.column_type(), is_unsigned),
            (MYSQL_TYPE_TINY, _)
                | (MYSQL_TYPE_SHORT, _)
                | (MYSQL_TYPE_YEAR, _)
                | (MYSQL_TYPE_LONG, false)
                | (MYSQL_TYPE_INT24, false)
        )
    }

    fn is_int64(&self) -> bool {
        use ColumnType::*;

        let is_unsigned = self.flags().intersects(ColumnFlags::UNSIGNED_FLAG);

        // https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary
        // MYSQL_TYPE_LONGLONG = i64
        // UNSIGNED MYSQL_TYPE_LONG = u32
        // UNSIGNED MYSQL_TYPE_INT24 = u32
        matches!(
            (self.column_type(), is_unsigned),
            (MYSQL_TYPE_LONGLONG, _) | (MYSQL_TYPE_LONG, true) | (MYSQL_TYPE_INT24, true)
        )
    }

    fn is_datetime(&self) -> bool {
        use ColumnType::*;

        matches!(
            self.column_type(),
            MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_DATETIME | MYSQL_TYPE_TIMESTAMP2 | MYSQL_TYPE_DATETIME2
        )
    }

    fn is_time(&self) -> bool {
        use ColumnType::*;

        matches!(self.column_type(), MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2)
    }

    fn is_date(&self) -> bool {
        use ColumnType::*;

        matches!(self.column_type(), MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE)
    }

    fn is_text(&self) -> bool {
        use ColumnType::*;

        let is_defined_text = matches!(
            self.column_type(),
            MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING
        );

        let is_bytes_but_text = matches!(
            self.column_type(),
            MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB
        ) && self.character_set() != 63;

        is_defined_text || is_bytes_but_text
    }

    fn is_bytes(&self) -> bool {
        use ColumnType::*;

        let is_bytes = matches!(
            self.column_type(),
            MYSQL_TYPE_TINY_BLOB
                | MYSQL_TYPE_MEDIUM_BLOB
                | MYSQL_TYPE_LONG_BLOB
                | MYSQL_TYPE_BLOB
                | MYSQL_TYPE_VAR_STRING
                | MYSQL_TYPE_STRING
        ) && self.character_set() == 63;

        let is_bits = self.column_type() == MYSQL_TYPE_BIT && self.column_length() > 1;

        is_bytes || is_bits
    }

    fn is_bool(&self) -> bool {
        // TODO:增加MYSQL_TYPE_TINY的长度为1的也识别为bool
        (self.column_type() == ColumnType::MYSQL_TYPE_BIT && self.column_length() == 1)
            || (self.column_type() == ColumnType::MYSQL_TYPE_TINY && self.column_length() <= 4)
    }

    fn is_json(&self) -> bool {
        self.column_type() == ColumnType::MYSQL_TYPE_JSON
    }

    fn is_enum(&self) -> bool {
        self.flags() == ColumnFlags::ENUM_FLAG || self.column_type() == ColumnType::MYSQL_TYPE_ENUM
    }

    fn is_null(&self) -> bool {
        self.column_type() == ColumnType::MYSQL_TYPE_NULL
    }
}

impl TakeRow for my::Row {
    fn take_result_row(&mut self) -> crate::Result<Vec<Value<'static>>> {
        fn convert(row: &mut my::Row, i: usize) -> crate::Result<Value<'static>> {
            let value = row.take(i).ok_or_else(|| {
                let msg = "Index out of bounds";
                let kind = ErrorKind::conversion(msg);

                Error::builder(kind).build()
            })?;

            let column = row.columns_ref().get(i).ok_or_else(|| {
                let msg = "Index out of bounds";
                let kind = ErrorKind::conversion(msg);

                Error::builder(kind).build()
            })?;
            // println!(
            //     "转换列:{},是否布尔:{},长度:{},类型:{}",
            //     column.name_str(),
            //     column.is_bool(),
            //     column.column_length(),
            //     column.column_type() == ColumnType::MYSQL_TYPE_TINY
            // );
            let res = match value {
                // JSON is returned as bytes.
                my::Value::Bytes(b) if column.is_json() => {
                    serde_json::from_slice(&b).map(Value::json).map_err(|_| {
                        let msg = "Unable to convert bytes to JSON";
                        let kind = ErrorKind::conversion(msg);

                        Error::builder(kind).build()
                    })?
                }
                my::Value::Bytes(b) if column.is_enum() => {
                    let s = String::from_utf8(b)?;
                    Value::enum_variant(s)
                }
                //增加二进制blob转日期类型
                my::Value::Bytes(b) if column.is_datetime() => {
                    let dt = parse_mysql_datetime_string(&b).unwrap();
                    // println!("解析时间：{}-{}-{} {}:{}:{}", dt.0, month, day, hour, minute, second);
                    let date = NaiveDate::from_ymd_opt(dt.0 as i32, dt.1, dt.2).unwrap();
                    let time = NaiveTime::from_hms_opt(dt.3, dt.4, dt.5).unwrap();
                    let dt = NaiveDateTime::new(date, time);
                    Value::datetime(DateTime::<Utc>::from_utc(dt, Utc))
                }
                //TODO 增加二进制blob转日期类型，适配海量数据库
                my::Value::Bytes(b) if column.is_date() => {
                    let dt = parse_mysql_datetime_string(&b).unwrap();
                    // println!("解析时间：{}-{}-{} {}:{}:{}", dt.0, month, day, hour, minute, second);
                    let date = NaiveDate::from_ymd_opt(dt.0 as i32, dt.1, dt.2).unwrap();
                    let time = NaiveTime::from_hms_opt(dt.3, dt.4, dt.5).unwrap();
                    let dt = NaiveDateTime::new(date, time);
                    Value::datetime(DateTime::<Utc>::from_utc(dt, Utc))
                }
                //TODO 增加二进制blob转日期类型，适配海量数据库
                my::Value::Bytes(b) if column.is_time() => {
                    let dt = parse_mysql_time_string(&b).unwrap();
                    if dt.0 {
                        let kind = ErrorKind::conversion("Failed to convert a negative time");
                        return Err(Error::builder(kind).build());
                    }
                    // if days != 0 {
                    //     let kind = ErrorKind::conversion("Failed to read a MySQL `time` as duration");
                    //     return Err(Error::builder(kind).build());
                    // }
                    let time = NaiveTime::from_hms_micro_opt(dt.1, dt.2, dt.3, dt.4).unwrap();
                    Value::time(time)
                }

                // TODO 适配starrocks数据库的select count(*)，返回的值是字节数组；
                my::Value::Bytes(b) if column.is_int64() => {
                    let s = String::from_utf8(b).map_err(|_| {
                        let msg = "Could not convert INT64 from bytes to String.";
                        let kind = ErrorKind::conversion(msg);

                        Error::builder(kind).build()
                    })?;
                    let parsed_value: i64 = s.parse().map_err(|_| {
                        let msg = "Could not parse string to INT64.";
                        let kind = ErrorKind::conversion(msg);

                        Error::builder(kind).build()
                    })?;
                    Value::int64(parsed_value)
                }

                my::Value::Bytes(b) if column.is_int32() => {
                    let s = String::from_utf8(b).map_err(|_| {
                        let msg = "Could not convert INT32 from bytes to String.";
                        let kind = ErrorKind::conversion(msg);

                        Error::builder(kind).build()
                    })?;
                    let parsed_value: i32 = s.parse().map_err(|_| {
                        let msg = "Could not parse string to INT32.";
                        let kind = ErrorKind::conversion(msg);

                        Error::builder(kind).build()
                    })?;
                    Value::int32(parsed_value)
                }

                // NEWDECIMAL returned as bytes. See https://mariadb.com/kb/en/resultset-row/#decimal-binary-encoding
                my::Value::Bytes(b) if column.is_real() => {
                    let s = String::from_utf8(b).map_err(|_| {
                        let msg = "Could not convert NEWDECIMAL from bytes to String.";
                        let kind = ErrorKind::conversion(msg);

                        Error::builder(kind).build()
                    })?;

                    let dec = s.parse().map_err(|_| {
                        let msg = "Could not convert NEWDECIMAL string to a BigDecimal.";
                        let kind = ErrorKind::conversion(msg);

                        Error::builder(kind).build()
                    })?;

                    Value::numeric(dec)
                }
                my::Value::Bytes(b) if column.is_bool() => match b.as_slice() {
                    [0] => Value::boolean(false),
                    _ => Value::boolean(true),
                },
                // https://dev.mysql.com/doc/internals/en/character-set.html
                my::Value::Bytes(b) if column.character_set() == 63 => Value::bytes(b),
                my::Value::Bytes(s) => Value::text(String::from_utf8(s)?),
                my::Value::Int(i) if column.is_int64() => Value::int64(i),
                my::Value::Int(i) => Value::int32(i as i32),
                my::Value::UInt(i) => Value::int64(i64::try_from(i).map_err(|_| {
                    let msg = "Unsigned integers larger than 9_223_372_036_854_775_807 are currently not handled.";
                    let kind = ErrorKind::value_out_of_range(msg);

                    Error::builder(kind).build()
                })?),
                my::Value::Float(f) => Value::from(f),
                my::Value::Double(f) => Value::from(f),
                my::Value::Date(year, month, day, _, _, _, _) if column.is_date() => {
                    if day == 0 || month == 0 {
                        let msg = format!(
                            "The column `{}` contained an invalid datetime value with either day or month set to zero.",
                            column.name_str()
                        );
                        let kind = ErrorKind::value_out_of_range(msg);
                        return Err(Error::builder(kind).build());
                    }

                    let date = NaiveDate::from_ymd_opt(year.into(), month.into(), day.into()).unwrap();

                    Value::date(date)
                }
                my::Value::Date(year, month, day, hour, min, sec, micro) => {
                    if day == 0 || month == 0 {
                        let msg = format!(
                            "The column `{}` contained an invalid datetime value with either day or month set to zero.",
                            column.name_str()
                        );
                        let kind = ErrorKind::value_out_of_range(msg);
                        return Err(Error::builder(kind).build());
                    }

                    let time = NaiveTime::from_hms_micro_opt(hour.into(), min.into(), sec.into(), micro).unwrap();

                    let date = NaiveDate::from_ymd_opt(year.into(), month.into(), day.into()).unwrap();
                    let dt = NaiveDateTime::new(date, time);

                    Value::datetime(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
                }
                my::Value::Time(is_neg, days, hours, minutes, seconds, micros) => {
                    if is_neg {
                        let kind = ErrorKind::conversion("Failed to convert a negative time");
                        return Err(Error::builder(kind).build());
                    }

                    if days != 0 {
                        let kind = ErrorKind::conversion("Failed to read a MySQL `time` as duration");
                        return Err(Error::builder(kind).build());
                    }

                    let time =
                        NaiveTime::from_hms_micro_opt(hours.into(), minutes.into(), seconds.into(), micros).unwrap();
                    Value::time(time)
                }
                my::Value::NULL => match column {
                    t if t.is_bool() => Value::null_boolean(),
                    t if t.is_enum() => Value::null_enum(),
                    t if t.is_null() => Value::null_int32(),
                    t if t.is_int64() => Value::null_int64(),
                    t if t.is_int32() => Value::null_int32(),
                    t if t.is_float() => Value::null_float(),
                    t if t.is_double() => Value::null_double(),
                    t if t.is_text() => Value::null_text(),
                    t if t.is_bytes() => Value::null_bytes(),

                    t if t.is_real() => Value::null_numeric(),
                    t if t.is_datetime() => Value::null_datetime(),
                    t if t.is_time() => Value::null_time(),
                    t if t.is_date() => Value::null_date(),
                    t if t.is_json() => Value::null_json(),
                    typ => {
                        let msg = format!("Value of type {typ:?} is not supported with the current configuration");

                        let kind = ErrorKind::conversion(msg);
                        return Err(Error::builder(kind).build());
                    }
                },
            };

            Ok(res)
        }

        let mut row = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            row.push(convert(self, i)?);
        }

        Ok(row)
    }
}
