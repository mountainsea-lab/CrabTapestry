use crate::external::crab_hmds::constant;
use crate::external::crab_hmds::meta::OhlcvRecord;
use barter_integration::protocol::http::rest::RestRequest;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct FetchCrabHmdsKlineRequest {
    pub(crate) query_params: BTreeMap<String, String>,
}

impl RestRequest for FetchCrabHmdsKlineRequest {
    type Response = CrabHmdsKlineResponse;
    type QueryParams = BTreeMap<String, String>;
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed(constant::KLINES)
    }

    fn method() -> reqwest::Method {
        reqwest::Method::GET
    }

    fn query_params(&self) -> Option<&Self::QueryParams> {
        Some(&self.query_params)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CrabHmdsKlineResponse(pub Vec<OhlcvRecord>);
