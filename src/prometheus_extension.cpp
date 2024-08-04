#include "duckdb/function/table_function.hpp"
#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "prometheus_extension.hpp"

#include <cstdio>
#include <iostream>

namespace duckdb {

struct PrometheusFunctionBindData : public TableFunctionData {
	std::string query;

	PrometheusFunctionBindData(std::string q) {
		query = q;
	};
};

unique_ptr<FunctionData> PrometheusBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto qry = StringValue::Get(input.inputs[0]);

	std::string api_url = "http://localhost:9090/api/v1/query";
	auto api_url_entry = input.named_parameters.find("api_url");
	if (api_url_entry != input.named_parameters.end()) {
		api_url = StringValue::Get(api_url_entry->second);
	}

	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("timestamp");

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("__name__");

	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.emplace_back("labels");

	return_types.emplace_back(LogicalType::FLOAT);
	names.emplace_back("value");

	auto res = make_uniq<PrometheusFunctionBindData>(qry);

	return std::move(res);
}

struct GlobalPrometheusTableFunctionState : public GlobalTableFunctionState {};

unique_ptr<GlobalTableFunctionState> PrometheusGlobalStateFunction(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto res = make_uniq<GlobalPrometheusTableFunctionState>();

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
