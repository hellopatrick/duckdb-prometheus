#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_size.hpp"

#include <cstddef>
#include <string>
#define DUCKDB_EXTENSION_MAIN
#define CPPHTTPLIB_OPENSSL_SUPPORT

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "httplib.hpp"
#include "prometheus_extension.hpp"
#include "yyjson.hpp"

#include <cstdio>
#include <iostream>

using namespace duckdb_yyjson;

namespace duckdb {

struct PrometheusFunctionBindData : public TableFunctionData {
	std::string query;
	std::string api_url;

	PrometheusFunctionBindData(std::string q, std::string u) {
		query = q;
		api_url = u;
	};
};

unique_ptr<FunctionData> PrometheusBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto qry = StringValue::Get(input.inputs[0]);

	std::string api_url = "http://localhost:9090";
	auto api_url_entry = input.named_parameters.find("api_url");
	if (api_url_entry != input.named_parameters.end()) {
		api_url = StringValue::Get(api_url_entry->second);
	}

	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("timestamp");

	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.emplace_back("labels");

	return_types.emplace_back(LogicalType::FLOAT);
	names.emplace_back("value");

	auto res = make_uniq<PrometheusFunctionBindData>(qry, api_url);

	return std::move(res);
}

struct Metric {
	std::unordered_map<string, string> labels;
	std::vector<std::pair<float, float>> values;
};

struct GlobalPrometheusTableFunctionState : public GlobalTableFunctionState {
	std::vector<Metric> metrics;
	size_t offset;
};

unordered_map<string, string> yyjson_obj_to_map(yyjson_val *obj) {
	unordered_map<string, string> result;
	yyjson_val *key, *val;
	size_t idx, max;

	// Iterate through the key-value pairs in the JSON object
	yyjson_obj_foreach(obj, idx, max, key, val) {
		string key_str = yyjson_get_str(key);
		string val_str = yyjson_get_str(val);
		result[key_str] = val_str;
	}

	return result;
}

unique_ptr<GlobalTableFunctionState> PrometheusGlobalStateFunction(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	using namespace duckdb_httplib_openssl;

	auto res = make_uniq<GlobalPrometheusTableFunctionState>();
	auto &bind_data = input.bind_data->Cast<PrometheusFunctionBindData>();

	Client cli(bind_data.api_url);
	Params params {{"query", bind_data.query}};

	auto r = cli.Post("/api/v1/query", params);

	yyjson_doc *doc = yyjson_read(r->body.c_str(), r->body.length(), 0);
	yyjson_val *root = yyjson_doc_get_root(doc);

	yyjson_val *data = yyjson_obj_get(root, "data");
	yyjson_val *result = yyjson_obj_get(data, "result");

	size_t idx, max;
	yyjson_val *val;

	std::vector<Metric> metrics;

	yyjson_arr_foreach(result, idx, max, val) {
		Metric m;

		// Extract "metric" object within the current result
		yyjson_val *metric = yyjson_obj_get(val, "metric");
		unordered_map<string, string> labels = yyjson_obj_to_map(metric);

		m.labels = labels;

		// Extract "value" array within the current result
		yyjson_val *value = yyjson_obj_get(val, "value");
		yyjson_val *timestamp = yyjson_arr_get(value, 0);
		yyjson_val *mv = yyjson_arr_get(value, 1);

		auto pp = std::pair<float, float>(yyjson_get_real(timestamp), std::stof(yyjson_get_str(mv)));

		m.values.push_back(pp);

		metrics.push_back(m);
	}

	yyjson_doc_free(doc);

	res->metrics = metrics;
	res->offset = 0;

	return std::move(res);
}

struct LocalPrometheusTableFunctionState : public LocalTableFunctionState {};

unique_ptr<LocalTableFunctionState> PrometheusLocalStateFunction(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state) {
	auto res = make_uniq<LocalPrometheusTableFunctionState>();

	return std::move(res);
}

void PrometheusTableFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlobalPrometheusTableFunctionState>();

	size_t i = 0;

	while (state.offset < state.metrics.size() && i < STANDARD_VECTOR_SIZE) {
		auto m = state.metrics[state.offset];

		size_t j = 0;

		while (j < m.values.size()) {
			size_t col = 0;
			auto pt = m.values[j];

			auto ms = static_cast<int64_t>(pt.first * 1000);
			auto val = pt.second;

			output.SetValue(col++, i, Value::TIMESTAMP(Timestamp::FromEpochMs(ms)));
			output.SetValue(col++, i, Value::MAP(m.labels));
			output.SetValue(col++, i, Value::FLOAT(val));

			j++;
		}

		state.offset += 1;
		i += 1;
	}

	output.SetCardinality(i);
}

static void LoadInternal(DatabaseInstance &instance) {
	auto &config = DBConfig::GetConfig(instance);

	auto prometheus_table_function =
	    TableFunction("prometheus", {LogicalType::VARCHAR}, PrometheusTableFunction, PrometheusBindFunction,
	                  PrometheusGlobalStateFunction, PrometheusLocalStateFunction);

	prometheus_table_function.named_parameters["api_url"] = LogicalType::VARCHAR;

	ExtensionUtil::RegisterFunction(instance, prometheus_table_function);
}

void PrometheusExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string PrometheusExtension::Name() {
	return "prometheus";
}

std::string PrometheusExtension::Version() const {

#ifdef EXT_VERSION_PROMETHEUS
	return EXT_VERSION_PROMETHEUS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void prometheus_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::PrometheusExtension>();
}

DUCKDB_EXTENSION_API const char *prometheus_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
