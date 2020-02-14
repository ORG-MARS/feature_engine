namespace cpp doc_property_thrift

struct GetDocPropertyRequest {
  1: required string rid;
  2: required list<string> docids;
  3: required set<string> property_names;
}

struct MultiType {
  1: optional string  str_val;
  2: optional i64     i64_val;
  3: optional double  double_val;
}

struct GetDocPropertyResponse {
  1: optional i32 err_code;
  2: optional string msg;
  3: optional map<string, map<string, MultiType>> properties;
}

service DocPropertyService {
  GetDocPropertyResponse GetDocProperty(1: GetDocPropertyRequest request)
}

