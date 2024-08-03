#define DUCKDB_EXTENSION_MAIN

#include "prometheus_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void PrometheusScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Prometheus "+name.GetString()+" 🐥");;
        });
}

inline void PrometheusOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Prometheus " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto prometheus_scalar_function = ScalarFunction("prometheus", {LogicalType::VARCHAR}, LogicalType::VARCHAR, PrometheusScalarFun);
    ExtensionUtil::RegisterFunction(instance, prometheus_scalar_function);

    // Register another scalar function
    auto prometheus_openssl_version_scalar_function = ScalarFunction("prometheus_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, PrometheusOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, prometheus_openssl_version_scalar_function);
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
